#!/usr/bin/env node
/**
 * rebuild-pmtiles-neuf.js
 *
 * 1. Télécharge batiment_groupe_dpe_representatif_logement.csv depuis le ZIP BDNB (partiel)
 * 2. Construit un lookup batiment_groupe_id → neuf (bool)
 * 3. Lit le PMTiles existant, décode chaque tuile MVT, ajoute la propriété `neuf` (0/1)
 * 4. Réécrit un nouveau fichier PMTiles
 *
 * Usage: node rebuild-pmtiles-neuf.js [input.pmtiles] [output.pmtiles]
 * Nécessite: npm install @mapbox/vector-tile pbf vt-pbf
 */

'use strict';

const https   = require('https');
const zlib    = require('zlib');
const fs      = require('fs');
const path    = require('path');

const VectorTile = require('@mapbox/vector-tile').VectorTile;
const Protobuf   = require('pbf').default;
const vtpbf      = require('vt-pbf');

// ─── Configuration ────────────────────────────────────────────────────────────
const BDNB_ZIP_URL     = 'https://open-data.s3.fr-par.scw.cloud/bdnb_millesime_2025-07-a/millesime_2025-07-a_dep44/open_data_millesime_2025-07-a_dep44_csv.zip';
const DPE_REP_COMP_SIZE = 23907257;
const DPE_REP_LH_OFFSET = 552862793;

const INPUT_PMTILES  = process.argv[2] || path.join(__dirname, '../public/dpe-44.pmtiles');
const OUTPUT_PMTILES = process.argv[3] || path.join(__dirname, '../public/dpe-44.pmtiles');

// ─── HTTP partial download ─────────────────────────────────────────────────────
function fetchRange(url, start, length) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { Range: `bytes=${start}-${start + length - 1}` } }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end',  () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    }).on('error', reject);
  });
}

// ─── Varint helpers (64-bit safe) ─────────────────────────────────────────────
// ATTENTION : les tileIds PMTiles au zoom ≥16 dépassent 2^31 et nécessitent
// une arithmétique 64-bit. Les opérateurs bit à bit JS sont 32-bit, donc on
// doit éviter |= et << pour les bits ≥32.
function readVarint(buf, pos) {
  // Accumule en double précision : les 28 premiers bits via |= (sans overflow),
  // les bits 28+ via multiplication flottante.
  let b, lo = 0, hi = 0;
  b = buf[pos.v++]; lo  = (b & 0x7f);        if (!(b & 0x80)) return lo;
  b = buf[pos.v++]; lo |= (b & 0x7f) << 7;   if (!(b & 0x80)) return lo;
  b = buf[pos.v++]; lo |= (b & 0x7f) << 14;  if (!(b & 0x80)) return lo;
  b = buf[pos.v++]; lo |= (b & 0x7f) << 21;  if (!(b & 0x80)) return lo;
  // bit 28 : 4 bits dans lo (pour rester signé 32-bit), 3 bits dans hi
  b = buf[pos.v++];
  lo |= (b & 0x0f) << 28;  // bits 28-31 dans lo (signé OK car ≤ 4 bits)
  hi  = (b & 0x7f) >> 4;   // bits 32-34 dans hi
  if (!(b & 0x80)) return (lo >>> 0) + hi * 0x100000000;
  b = buf[pos.v++]; hi |= (b & 0x7f) << 3;   if (!(b & 0x80)) return (lo >>> 0) + hi * 0x100000000;
  b = buf[pos.v++]; hi |= (b & 0x7f) << 10;  if (!(b & 0x80)) return (lo >>> 0) + hi * 0x100000000;
  b = buf[pos.v++]; hi |= (b & 0x7f) << 17;  if (!(b & 0x80)) return (lo >>> 0) + hi * 0x100000000;
  b = buf[pos.v++]; hi |= (b & 0x7f) << 24;  if (!(b & 0x80)) return (lo >>> 0) + hi * 0x100000000;
  b = buf[pos.v++]; hi |= (b & 0x01) << 31;
  return (lo >>> 0) + hi * 0x100000000;
}

function writeVarint(n) {
  // n peut être un grand entier JS (float64) — on utilise % et / flottants,
  // et & 0x7f qui fonctionne car 2^32 est divisible par 128.
  const bytes = [];
  while (n > 0x7f) {
    bytes.push((n & 0x7f) | 0x80);
    n = Math.floor(n / 128);
  }
  bytes.push(n & 0x7f);
  return Buffer.from(bytes);
}

// ─── PMTiles directory ────────────────────────────────────────────────────────
function parseDir(compressedBuf) {
  const raw = zlib.gunzipSync(compressedBuf);
  const pos = { v: 0 };
  const n = readVarint(raw, pos);
  const entries = [];
  let lastId = 0;
  for (let i = 0; i < n; i++) {
    const delta = readVarint(raw, pos);
    entries.push({ tileId: lastId + delta, runLength: 0, length: 0, offset: 0 });
    lastId = entries[i].tileId;
  }
  for (let i = 0; i < n; i++) entries[i].runLength = readVarint(raw, pos);
  for (let i = 0; i < n; i++) entries[i].length     = readVarint(raw, pos);
  for (let i = 0; i < n; i++) {
    const v = readVarint(raw, pos);
    entries[i].offset = (v === 0 && i > 0) ? entries[i-1].offset + entries[i-1].length : v - 1;
  }
  return entries;
}

function buildDir(entries) {
  const parts = [writeVarint(entries.length)];
  let lastId = 0;
  for (const e of entries) { parts.push(writeVarint(e.tileId - lastId)); lastId = e.tileId; }
  for (const e of entries)   parts.push(writeVarint(e.runLength));
  for (const e of entries)   parts.push(writeVarint(e.length));
  for (let i = 0; i < entries.length; i++) {
    const prev = entries[i-1];
    if (i > 0 && entries[i].offset === prev.offset + prev.length) {
      parts.push(writeVarint(0));
    } else {
      parts.push(writeVarint(entries[i].offset + 1));
    }
  }
  return zlib.gzipSync(Buffer.concat(parts));
}

// ─── PMTiles header ───────────────────────────────────────────────────────────
function parseHeader(buf) {
  const v = new DataView(buf.buffer, buf.byteOffset, 127);
  const u64 = (off) => v.getUint32(off+4, true) * 0x100000000 + v.getUint32(off, true);
  return {
    specVersion:         buf[7],
    rootDirectoryOffset: u64(8),
    rootDirectoryLength: u64(16),
    jsonMetadataOffset:  u64(24),
    jsonMetadataLength:  u64(32),
    leafDirectoryOffset: u64(40),
    leafDirectoryLength: u64(48),
    tileDataOffset:      u64(56),
    tileDataLength:      u64(64),
    numAddressedTiles:   u64(72),
    numTileEntries:      u64(80),
    numTileContents:     u64(88),
    clustered:           buf[96] === 1,
    internalCompression: buf[97],
    tileCompression:     buf[98],
    tileType:            buf[99],
    minZoom:             buf[100],
    maxZoom:             buf[101],
    minLon:              v.getInt32(102, true) / 1e7,
    minLat:              v.getInt32(106, true) / 1e7,
    maxLon:              v.getInt32(110, true) / 1e7,
    maxLat:              v.getInt32(114, true) / 1e7,
    centerZoom:          buf[118],
    centerLon:           v.getInt32(119, true) / 1e7,
    centerLat:           v.getInt32(123, true) / 1e7,
  };
}

function buildHeader(h) {
  const buf = Buffer.alloc(127);
  buf.write('PMTiles', 0, 'ascii');
  buf[7] = h.specVersion;
  const v = new DataView(buf.buffer, buf.byteOffset, 127);
  const u64 = (off, val) => {
    v.setUint32(off,   val >>> 0,                          true);
    v.setUint32(off+4, Math.floor(val / 0x100000000) >>> 0, true);
  };
  u64(8,  h.rootDirectoryOffset);  u64(16, h.rootDirectoryLength);
  u64(24, h.jsonMetadataOffset);   u64(32, h.jsonMetadataLength);
  u64(40, h.leafDirectoryOffset);  u64(48, h.leafDirectoryLength);
  u64(56, h.tileDataOffset);       u64(64, h.tileDataLength);
  u64(72, h.numAddressedTiles);    u64(80, h.numTileEntries);
  u64(88, h.numTileContents);
  buf[96]  = h.clustered ? 1 : 0;
  buf[97]  = h.internalCompression;
  buf[98]  = h.tileCompression;
  buf[99]  = h.tileType;
  buf[100] = h.minZoom;  buf[101] = h.maxZoom;
  v.setInt32(102, Math.round(h.minLon * 1e7), true);
  v.setInt32(106, Math.round(h.minLat * 1e7), true);
  v.setInt32(110, Math.round(h.maxLon * 1e7), true);
  v.setInt32(114, Math.round(h.maxLat * 1e7), true);
  buf[118] = h.centerZoom;
  v.setInt32(119, Math.round(h.centerLon * 1e7), true);
  v.setInt32(123, Math.round(h.centerLat * 1e7), true);
  return buf;
}

// ─── MVT re-encode with neuf property ────────────────────────────────────────
function reEncodeTile(gzipBuf, neufLookup) {
  let raw;
  try { raw = zlib.gunzipSync(gzipBuf); } catch { raw = gzipBuf; }

  const srcTile = new VectorTile(new Protobuf(raw));

  // Build wrapper tile for vtpbf.fromVectorTileJs
  const wrappedTile = { layers: {} };
  for (const layerName of Object.keys(srcTile.layers)) {
    const layer = srcTile.layers[layerName];
    const wrappedFeats = [];
    for (let i = 0; i < layer.length; i++) {
      const feat = layer.feature(i);
      const extraProps = layerName === 'dpe' ? { neuf: neufLookup.get(feat.properties.id || '') ? 1 : 0 } : {};
      wrappedFeats.push({
        id: feat.id,
        type: feat.type,
        properties: { ...feat.properties, ...extraProps },
        loadGeometry: () => feat.loadGeometry(),
      });
    }
    wrappedTile.layers[layerName] = {
      name: layerName,
      extent: layer.extent,
      version: layer.version,
      length: wrappedFeats.length,
      feature: (i) => wrappedFeats[i],
    };
  }

  const newRaw = Buffer.from(vtpbf.fromVectorTileJs(wrappedTile));
  return zlib.gzipSync(newRaw);
}

// ─── Download and parse BDNB DPE representatif CSV ───────────────────────────
async function buildNeufLookup() {
  console.log('📥 Lecture de l\'en-tête local du fichier ZIP BDNB...');
  const lhBuf = await fetchRange(BDNB_ZIP_URL, DPE_REP_LH_OFFSET, 100);
  const fnLen = lhBuf.readUInt16LE(26);
  const exLen = lhBuf.readUInt16LE(28);
  const dataStart = DPE_REP_LH_OFFSET + 30 + fnLen + exLen;

  console.log(`📥 Téléchargement CSV DPE representatif (${Math.round(DPE_REP_COMP_SIZE/1024/1024)} MB)...`);
  const compressed = await fetchRange(BDNB_ZIP_URL, dataStart, DPE_REP_COMP_SIZE);

  console.log('🔓 Décompression...');
  const raw = zlib.inflateRawSync(compressed);

  console.log('📊 Parsing CSV...');
  const lines = raw.toString('utf8').split('\n');
  const sep   = lines[0].includes(';') ? ';' : ',';
  const hdrs  = lines[0].split(sep);

  const idIdx   = hdrs.indexOf('batiment_groupe_id');
  const typeIdx = hdrs.indexOf('type_dpe');

  if (idIdx === -1 || typeIdx === -1) {
    console.error('Colonnes disponibles:', hdrs.join(', '));
    throw new Error(`Colonnes manquantes: id=${idIdx}, type_dpe=${typeIdx}`);
  }

  const lookup = new Map(); // id → true (neuf)
  let neufCount = 0;

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const cols   = line.split(sep);
    const id     = (cols[idIdx] || '').replace(/^"|"$/g, '').trim();
    if (!id) continue;
    const typeDpe = (cols[typeIdx] || '').toLowerCase();
    const neuf    = typeDpe.includes('rt2012') || typeDpe.includes('re2020');
    if (neuf) { lookup.set(id, true); neufCount++; }
  }

  console.log(`✅ ${lines.length - 1} lignes parsées, ${neufCount} bâtiments neufs trouvés`);
  return lookup;
}

// ─── Read all tile entries from PMTiles ───────────────────────────────────────
function getAllTileEntries(pmBuf, header) {
  const rootComp   = pmBuf.slice(header.rootDirectoryOffset, header.rootDirectoryOffset + header.rootDirectoryLength);
  const rootEntries = parseDir(rootComp);
  const tileEntries = [];

  for (const re of rootEntries) {
    if (re.runLength === 0) {
      // Leaf dir reference
      const leafStart = header.leafDirectoryOffset + re.offset;
      const leafComp  = pmBuf.slice(leafStart, leafStart + re.length);
      const leafEntries = parseDir(leafComp);
      for (const le of leafEntries) {
        if (le.runLength > 0) tileEntries.push(le);
      }
    } else {
      tileEntries.push(re);
    }
  }
  return tileEntries;
}

// ─── Build new PMTiles ────────────────────────────────────────────────────────
function assemblePMTiles(header, metadata, tiles) {
  // tiles: [{tileId, data: Buffer}], sorted by tileId

  // Deduplicate consecutive identical data buffers (run-length encoding)
  // For simplicity we just write each tile individually (no dedup)
  const LEAF_SIZE = 512;

  // Build tile data and flat entries
  const dataBuffers = [];
  const flatEntries = [];
  let offset = 0;

  for (const { tileId, data } of tiles) {
    flatEntries.push({ tileId, offset, length: data.length, runLength: 1 });
    dataBuffers.push(data);
    offset += data.length;
  }
  const tileDataBuf = Buffer.concat(dataBuffers);

  // Build directories
  let rootDirBuf, leafDirsBuf;

  if (flatEntries.length <= LEAF_SIZE) {
    rootDirBuf  = buildDir(flatEntries);
    leafDirsBuf = Buffer.alloc(0);
  } else {
    // Split into leaf dirs
    const leafBufs = [];
    const rootEntries = [];
    let leafOff = 0;

    for (let i = 0; i < flatEntries.length; i += LEAF_SIZE) {
      const chunk = flatEntries.slice(i, i + LEAF_SIZE);
      const buf   = buildDir(chunk);
      rootEntries.push({ tileId: chunk[0].tileId, offset: leafOff, length: buf.length, runLength: 0 });
      leafBufs.push(buf);
      leafOff += buf.length;
    }

    rootDirBuf  = buildDir(rootEntries);
    leafDirsBuf = Buffer.concat(leafBufs);
  }

  // Metadata
  const metaBuf = zlib.gzipSync(Buffer.from(JSON.stringify(metadata)));

  // Section offsets
  const HEADER = 127;
  const rootDirectoryOffset  = HEADER;
  const rootDirectoryLength  = rootDirBuf.length;
  const jsonMetadataOffset   = rootDirectoryOffset + rootDirectoryLength;
  const jsonMetadataLength   = metaBuf.length;
  const leafDirectoryOffset  = jsonMetadataOffset + jsonMetadataLength;
  const leafDirectoryLength  = leafDirsBuf.length;
  const tileDataOffset       = leafDirectoryOffset + leafDirectoryLength;
  const tileDataLength       = tileDataBuf.length;

  const newHeader = {
    ...header,
    rootDirectoryOffset,  rootDirectoryLength,
    jsonMetadataOffset,   jsonMetadataLength,
    leafDirectoryOffset,  leafDirectoryLength,
    tileDataOffset,       tileDataLength,
    numAddressedTiles: tiles.length,
    numTileEntries:    flatEntries.length,
    numTileContents:   tiles.length,
    clustered: true,
  };

  return Buffer.concat([buildHeader(newHeader), rootDirBuf, metaBuf, leafDirsBuf, tileDataBuf]);
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  // 1. Build lookup
  const neufLookup = await buildNeufLookup();

  // 2. Read existing PMTiles
  console.log(`\n📖 Lecture de ${INPUT_PMTILES}...`);
  const pmBuf  = fs.readFileSync(INPUT_PMTILES);
  const header = parseHeader(pmBuf);
  console.log(`  ${Math.round(pmBuf.length/1024/1024)} MB, zoom ${header.minZoom}–${header.maxZoom}, ` +
              `${header.numTileContents} tuiles uniques`);

  // 3. Read metadata JSON
  const metaComp = pmBuf.slice(header.jsonMetadataOffset, header.jsonMetadataOffset + header.jsonMetadataLength);
  const metadata  = JSON.parse(zlib.gunzipSync(metaComp).toString('utf8'));
  // Add neuf field to vector_layers metadata
  const dpeLayer = (metadata.vector_layers || []).find(l => l.id === 'dpe');
  if (dpeLayer) { dpeLayer.fields = { ...(dpeLayer.fields || {}), neuf: 'Number' }; }

  // 4. Get all tile entries
  console.log('\n🗂️  Lecture des répertoires...');
  const tileEntries = getAllTileEntries(pmBuf, header);
  console.log(`  ${tileEntries.length} entrées de tuiles`);

  // 5. Process tiles
  console.log('\n🔄 Re-encodage des tuiles...');
  const processedMap = new Map(); // original offset → new Buffer (dedup)
  const tiles = [];
  let done = 0;

  for (const entry of tileEntries) {
    let newBuf;
    if (processedMap.has(entry.offset)) {
      newBuf = processedMap.get(entry.offset);
    } else {
      const raw = pmBuf.slice(header.tileDataOffset + entry.offset, header.tileDataOffset + entry.offset + entry.length);
      newBuf = reEncodeTile(raw, neufLookup);
      processedMap.set(entry.offset, newBuf);
    }

    // Expand run-length entries (usually runLength=1 for all tiles)
    for (let r = 0; r < entry.runLength; r++) {
      tiles.push({ tileId: entry.tileId + r, data: newBuf });
    }

    done += entry.runLength;
    if (done % 5000 === 0) process.stdout.write(`  ${done}...\r`);
  }

  tiles.sort((a, b) => a.tileId - b.tileId);
  console.log(`  ✅ ${tiles.length} tuiles traitées (${processedMap.size} blobs uniques)`);

  // 6. Assemble & write
  console.log('\n📦 Assemblage du nouveau PMTiles...');
  const newFile = assemblePMTiles(header, metadata, tiles);
  console.log(`  Taille: ${Math.round(newFile.length/1024/1024)} MB`);

  const tmp = OUTPUT_PMTILES + '.tmp';
  fs.writeFileSync(tmp, newFile);
  fs.renameSync(tmp, OUTPUT_PMTILES);
  console.log(`\n✅ Fichier écrit : ${OUTPUT_PMTILES}`);
  console.log('🎉 Terminé !');
}

main().catch(err => { console.error('❌ Erreur:', err); process.exit(1); });
