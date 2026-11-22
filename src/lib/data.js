// Accès aux données (repris de la version précédente de Scope, sans changement de méthode).
// Règle stricte : un bâtiment ou un DPE n'est affiché que s'il est rattaché de façon certaine.
import booleanPointInPolygon from '@turf/boolean-point-in-polygon';
import { point } from '@turf/helpers';
import { R2_BASE } from './config.js';

const json = async (url, opts) => {
  const r = await fetch(url, opts);
  if (!r.ok) throw new Error('HTTP ' + r.status);
  return r.json();
};

// ── Adresses (BAN) ──
export async function searchAddress(q, center, limit = 6) {
  const p = center ? `&lat=${center.lat.toFixed(4)}&lon=${center.lng.toFixed(4)}` : '';
  const d = await json(`https://api-adresse.data.gouv.fr/search/?q=${encodeURIComponent(q)}${p}&limit=${limit}`);
  return d.features || [];
}
export async function reverseAddress(lng, lat) {
  const d = await json(`https://api-adresse.data.gouv.fr/reverse/?lon=${lng}&lat=${lat}`);
  return d.features?.[0] || null;
}

// ── Cadastre ──
// Parcelle à partir de son identifiant (14 caractères : commune 5, préfixe 3, section 2, numéro 4)
export function parcelFromIdu(idu) {
  if (!idu || idu.length !== 14) return null;
  return { idu, code_insee: idu.slice(0, 5), section: idu.slice(8, 10).replace(/^0+(?=[A-Z])/, ''),
           numero: idu.slice(10).replace(/^0+/, ''), confident: true };
}
export async function fetchCadastre(lat, lng) {
  const d = 0.0002; // ~20 m
  try {
    const js = await json(`https://data.geopf.fr/wfs?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature`
      + `&TYPENAMES=CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle&OUTPUTFORMAT=application/json&COUNT=10`
      + `&BBOX=${lng - d},${lat - d},${lng + d},${lat + d},EPSG:4326`);
    if (!js.features?.length) return null;
    const pt = point([lng, lat]);
    const pip = js.features.find(f => { try { return booleanPointInPolygon(pt, f); } catch { return false; } });
    const match = pip || js.features[0];
    const confident = !!pip || js.features.length === 1;
    const p = match?.properties;
    if (!p) return null;
    const section = (p.section || '').replace(/^0+(?=[A-Z])/, '') || p.section || '';
    const numero = (p.numero || '').replace(/^0+/, '') || p.numero || '';
    return { idu: p.idu || '', section, numero, code_insee: p.code_insee || '', contenance: p.contenance || null, confident };
  } catch { return null; }
}

// ── Bâtiment (BDNB, via /api/bdnb) ──
// Chaque appel porte la clé de la fiche (comptée une fois par le serveur) et la session éventuelle.
export class LimiteAtteinte extends Error {
  constructor(info) { super('limite'); this.info = info; }
}
async function bdnbGet(query, fiche, renouvele = false) {
  const headers = {};
  let sbc = null;
  try {
    ({ sb: sbc } = await import('./compte.js'));
    const s = sbc ? (renouvele ? (await sbc.auth.refreshSession()).data.session : (await sbc.auth.getSession()).data.session) : null;
    if (s?.access_token) headers.Authorization = `Bearer ${s.access_token}`;
  } catch { /* sans compte */ }
  const r = await fetch(`/api/bdnb?${query}&fiche=${encodeURIComponent(fiche)}`, { headers });
  if (r.status === 401 && !renouvele && headers.Authorization) return bdnbGet(query, fiche, true); // jeton expiré
  if (r.status === 402) throw new LimiteAtteinte(await r.json().catch(() => ({})));
  if (!r.ok) return [];
  const data = await r.json();
  return Array.isArray(data) ? data : [];
}
const bdnbBbox = (lng, lat, d, fiche) => bdnbGet(`bbox=${encodeURIComponent(`${lng - d},${lat - d},${lng + d},${lat + d}`)}`, fiche);

/**
 * Trouve le bâtiment et la parcelle d'une adresse ou d'un clic.
 * Adresse saisie : bâtiment auquel la BDNB rattache l'identifiant BAN (méthode Go-Rénove), sinon aucun.
 * Clic : bâtiment dont le contour contient le point ; hors bâtiment, seulement s'il est seul sur la parcelle.
 */
export async function resolveBuilding({ lat, lng, feature, bldGroupId, isClick }) {
  const searchedBanId = !isClick && feature?.properties?.type === 'housenumber' ? (feature.properties.id || null) : null;
  const pickBest = list => {
    if (!list?.length) return null;
    if (bldGroupId) { const b = list.find(x => x.batiment_groupe_id === bldGroupId); if (b) return b; }
    if (searchedBanId) {
      const b = list.find(x => x.cle_interop_adr_principale_ban === searchedBanId
        || (Array.isArray(x.l_cle_interop_adr) && x.l_cle_interop_adr.includes(searchedBanId)));
      if (b) return b;
    }
    return null; // pas de rattachement certain : on n'affiche pas un voisin
  };

  // Clé de la fiche pour le quota : l'adresse saisie, ou le bâtiment / le point cliqué (≈ 10 m)
  const fiche = searchedBanId || bldGroupId || `${lng.toFixed(4)},${lat.toFixed(4)}`;
  let bdnb = null, cadastre = null;
  try {
    if (isClick) {
      cadastre = await fetchCadastre(lat, lng);
      if (bldGroupId) bdnb = pickBest(await bdnbBbox(lng, lat, 0.0003, fiche));
      if (!bdnb) {
        const inside = await bdnbGet(`point=${lng},${lat}`, fiche);
        if (inside.length) bdnb = inside[0];
      }
      if (!bdnb && cadastre?.idu && cadastre.confident) {
        const onParcel = (await bdnbBbox(lng, lat, 0.0005, fiche))
          .filter(b => Array.isArray(b.l_parcelle_id) && b.l_parcelle_id.includes(cadastre.idu));
        if (onParcel.length === 1) bdnb = onParcel[0];
      }
    } else {
      const [b, c] = await Promise.all([
        (async () => {
          for (const d of [0.0001, 0.0003, 0.0008]) {
            const found = pickBest(await bdnbBbox(lng, lat, d, fiche));
            if (found) return found;
          }
          return null;
        })(),
        fetchCadastre(lat, lng),
      ]);
      bdnb = b; cadastre = c;
      // Point d'adresse côté route : on prend la parcelle que la BDNB associe au bâtiment
      const parcels = Array.isArray(bdnb?.l_parcelle_id) ? bdnb.l_parcelle_id : [];
      if (parcels.length && !parcels.includes(cadastre?.idu)) cadastre = parcelFromIdu(parcels[0]) || cadastre;
    }
  } catch (e) {
    if (e instanceof LimiteAtteinte) throw e; // quota atteint : la fiche affiche la limite
    /* sinon : fiche sans bâtiment */
  }

  const mainBan = bdnb?.cle_interop_adr_principale_ban || null;
  const banIds = Array.isArray(bdnb?.l_cle_interop_adr) ? bdnb.l_cle_interop_adr : (mainBan ? [mainBan] : []);
  const bdnbIdu = Array.isArray(bdnb?.l_parcelle_id) ? bdnb.l_parcelle_id[0] : null;
  const idu = isClick ? (cadastre?.idu || bdnbIdu) : (bdnbIdu || cadastre?.idu);

  const props = feature?.properties || {};
  const street = (props.street || props.name || '').trim();
  const SKIP = /^(rue|avenue|boulevard|allée|allee|impasse|place|square|chemin|route|résidence|residence|voie|passage|cité|cite|hameau|lotissement|quartier|villa|domaine|du|de|la|le|les|des|d|l)$/i;
  const rnc = (props.citycode || bdnbIdu) ? {
    idu, cc: props.citycode || '', hn: (props.housenumber || '').trim(),
    streetKey: street.split(/\s+/).find(w => w.length >= 4 && !SKIP.test(w)) || '', lat, lng,
  } : null;

  return {
    bdnb, cadastre, idu, rnc,
    batId: bdnb?.batiment_groupe_id || null,
    // DPE : uniquement les adresses que la BDNB rattache au bâtiment (ou l'adresse saisie elle-même)
    banIds: banIds.length ? banIds : (!isClick && props.id ? [props.id] : []),
  };
}

// ── DPE (ADEME) ──
// Champs communs aux deux jeux ADEME ; les champs en plus ne sont demandés qu'au jeu des logements existants
const SEL = 'numero_dpe,date_reception_dpe,etiquette_dpe,surface_habitable_logement,surface_habitable_immeuble,nombre_appartement,methode_application_dpe,type_energie_principale_chauffage,type_energie_principale_ecs,type_installation_chauffage_n1,type_installation_ecs_n1,adresse_brut,code_postal_ban,annee_construction,periode_construction,conso_5_usages_par_m2_ep';
const SEL_EXISTANT = SEL + ',etiquette_ges,emission_ges_5_usages_par_m2,complement_adresse_logement';

async function ademeGet(url) {
  for (let i = 0; i < 3; i++) {
    if (i > 0) await new Promise(ok => setTimeout(ok, 1000 * i));
    const r = await fetch(url);
    if (r.status === 429) continue; // limite de débit : on réessaie
    if (!r.ok) return null;
    return r.json();
  }
  return null;
}
async function dpeDataset(dataset, size, banIds, select = SEL) {
  const base = `https://data.ademe.fr/data-fair/api/v1/datasets/${dataset}/lines?size=${size}&select=${select}&sort=-date_reception_dpe`;
  const all = [], seen = new Set();
  for (let i = 0; i < banIds.length; i += 30) {
    const ids = banIds.slice(i, i + 30).map(encodeURIComponent).join(',');
    const d = await ademeGet(`${base}&identifiant_ban_in=${ids}`);
    for (const r of d?.results || []) if (!seen.has(r.numero_dpe)) { seen.add(r.numero_dpe); all.push(r); }
  }
  return all.sort((a, b) => String(b.date_reception_dpe || '').localeCompare(String(a.date_reception_dpe || '')));
}
export async function fetchDPE(banIds) {
  const ids = [...new Set(banIds)];
  if (!ids.length) return { existants: [], neufs: [] };
  // Séquentiel pour éviter les rafales (erreurs 429)
  const existants = await dpeDataset('dpe03existant', 100, ids, SEL_EXISTANT).catch(() => []);
  const neufs = await dpeDataset('g3cgx7jb3cmys5voxz1mrm22', 50, ids).catch(() => []);
  return { existants, neufs };
}

// ── Copropriété (RNIC) ──
export async function fetchRNC({ idu, cc, hn, streetKey, lat, lng }) {
  const BASE = 'https://tabular-api.data.gouv.fr/api/resources/3ea8e2c3-0038-464a-b17e-cd5c91f65ce2/data/';
  const closest = (data, maxDistSq) => {
    let best = null, bestDist = Infinity;
    for (const e of data) if (e.lat != null && e.long != null) {
      const d = (e.long - lng) ** 2 + (e.lat - lat) ** 2;
      if (d < bestDist) { bestDist = d; best = e; }
    }
    return best && bestDist < maxDistSq ? best : null;
  };
  if (idu) {
    for (const n of [1, 2, 3]) {
      const r = await fetch(`${BASE}?page_size=5&reference_cadastrale_${n}__exact=${encodeURIComponent(idu)}`);
      if (r.ok) { const d = await r.json(); if (d.data?.length) return d.data[0]; }
    }
  }
  if (!cc) return null;
  if (hn) {
    const r = await fetch(`${BASE}?page_size=50&code_officiel_commune__exact=${cc}&adresse_reference__contains=${encodeURIComponent(hn)}`);
    if (r.ok) {
      const d = await r.json();
      const cands = (d.data || []).filter(e => { const a = (e.adresse_reference || '').trim(); return a === hn || a.startsWith(hn + ' ') || a.startsWith(hn + ','); });
      const res = closest(cands, 0.000003);
      if (res) return res;
    }
  }
  if (streetKey) {
    const key = streetKey.normalize('NFD').replace(/[̀-ͯ]/g, '');
    const r = await fetch(`${BASE}?page_size=100&code_officiel_commune__exact=${cc}&adresse_reference__contains=${encodeURIComponent(key)}`);
    if (r.ok) { const d = await r.json(); return closest(d.data || [], 0.0000008); }
  }
  return null;
}

// ── Urbanisme (Géoportail de l'urbanisme) ──
export async function fetchPLUZone(lat, lng) {
  const geom = encodeURIComponent(JSON.stringify({ type: 'Point', coordinates: [lng, lat] }));
  const d = await json(`https://apicarto.ign.fr/api/gpu/zone-urba?geom=${geom}`);
  return d?.features?.[0]?.properties || null;
}
export async function fetchPLUBounds(b) {
  const geom = encodeURIComponent(JSON.stringify({ type: 'Polygon', coordinates: [[
    [b.getWest(), b.getSouth()], [b.getEast(), b.getSouth()], [b.getEast(), b.getNorth()], [b.getWest(), b.getNorth()], [b.getWest(), b.getSouth()]]] }));
  return json(`https://apicarto.ign.fr/api/gpu/zone-urba?geom=${geom}&_limit=200`);
}

// ── Transactions (DVF, via /api/dvf) ──
export async function fetchDVF(idu) {
  const r = await fetch(`/api/dvf?idpar=${encodeURIComponent(idu)}`);
  return r.ok ? r.json() : null;
}

// ── Vue de rue Google (API Maps Embed officielle, clé limitée au domaine de Scope) ──
const GMAPS_KEY = import.meta.env.VITE_GOOGLE_MAPS_KEY || '';
export const hasGoogleStreetView = () => !!GMAPS_KEY;
const bearing = ([lng1, lat1], [lng2, lat2]) => {
  const r = Math.PI / 180, y = Math.sin((lng2 - lng1) * r) * Math.cos(lat2 * r);
  const x = Math.cos(lat1 * r) * Math.sin(lat2 * r) - Math.sin(lat1 * r) * Math.cos(lat2 * r) * Math.cos((lng2 - lng1) * r);
  return Math.round(((Math.atan2(y, x) / r) + 360) % 360);
};
// Cherche la photo prise depuis la rue la plus proche (voiture Google : « © Google »),
// en écartant les photos d'intérieur et les photos de particuliers. Métadonnées gratuites.
export async function googleStreetView(lng, lat) {
  if (!GMAPS_KEY) return { found: false };
  const base = `https://www.google.com/maps/embed/v1/streetview?key=${encodeURIComponent(GMAPS_KEY)}&language=fr&fov=80`;
  const meta = (la, ln, radius) => json(`https://maps.googleapis.com/maps/api/streetview/metadata?location=${la},${ln}&radius=${radius}&source=outdoor&key=${encodeURIComponent(GMAPS_KEY)}`).catch(() => null);
  const isCar = m => m?.status === 'OK' && m.pano_id && /google/i.test(m.copyright || '');
  const dist = m => Math.hypot((m.location.lng - lng) * Math.cos(lat * Math.PI / 180), m.location.lat - lat);

  let best = null;
  const first = await meta(lat, lng, 60);
  if (first?.status === 'ZERO_RESULTS') {
    // rien à 60 m : on élargit une fois
    const far = await meta(lat, lng, 150);
    if (isCar(far)) best = far;
  } else if (isCar(first)) {
    best = first;
  } else {
    // La photo la plus proche n'est pas une photo de rue Google : on sonde autour du bâtiment (25 et 50 m)
    const d = m => m / 111320;
    const probes = [];
    for (const r of [25, 50]) for (let k = 0; k < 8; k++) {
      const a = (k * Math.PI) / 4;
      probes.push(meta(lat + d(r) * Math.sin(a), lng + (d(r) * Math.cos(a)) / Math.cos(lat * Math.PI / 180), 40));
    }
    const cands = (await Promise.all(probes)).filter(isCar);
    best = cands.sort((x, y) => dist(x) - dist(y))[0] || null;
  }
  if (!best) return { found: false };
  const h = bearing([best.location.lng, best.location.lat], [lng, lat]);
  const q = `pano=${encodeURIComponent(best.pano_id)}&heading=${h}&pitch=5&fov=80&key=${encodeURIComponent(GMAPS_KEY)}`;
  return {
    found: true,
    url: `${base}&pano=${encodeURIComponent(best.pano_id)}&heading=${h}&pitch=5`,
    // Aperçu fixe (API Street View Static : 10 000 images gratuites par mois, payant au-delà)
    preview: `https://maps.googleapis.com/maps/api/streetview?size=640x320&scale=2&return_error_code=true&${q}`,
    date: best.date || null,
  };
}

// ── Fichiers de tuiles nationales sur R2 ──
export async function r2Meta(couche) {
  return json(`${R2_BASE}/${couche}-meta.json`, { cache: 'no-store' });
}

// Altitude du terrain (RGE ALTI, IGN), en mètres
export async function fetchAltitude(lng, lat) {
  const u = `https://data.geopf.fr/altimetrie/1.0/calcul/alti/rest/elevation.json?lon=${lng}&lat=${lat}&resource=ign_rge_alti_wld&zonly=true`;
  const r = await fetch(u);
  if (!r.ok) throw new Error('altitude');
  const z = (await r.json())?.elevations?.[0];
  return typeof z === 'number' && z > -1000 ? z : null;
}
