// Mes DPE : les DPE réalisés par l'utilisateur, importés depuis l'export de son espace ADEME
// (CSV, XLSX ou XML). L'export ne contient pas l'adresse : pour chaque n° DPE, Scope interroge
// l'API ADEME. Tout reste sur l'appareil (localStorage), rien n'est partagé.
const KEY = 'scope.mesdpe.v1';
const BASE = 'https://data.ademe.fr/data-fair/api/v1/datasets';
const FIELDS = 'numero_dpe,date_etablissement_dpe,date_reception_dpe,etiquette_dpe,etiquette_ges,adresse_ban,adresse_brut,code_postal_ban,code_insee_ban,nom_commune_ban,identifiant_ban,score_ban,_geopoint,complement_adresse_batiment';
export const SCORE_BAS = 0.8; // en dessous, l'adresse géocodée est à vérifier

// n° DPE : 4 chiffres, 1 lettre, 7 chiffres, 1 lettre (tertiaire : préfixe T, traité à part)
const RE_NUM = /\b(T?\d{4}[A-Z]\d{7}[A-Z])\b/g;
export const isReplaced = x => /remplac/i.test(x.statut || '');
const norm = s => String(s ?? '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().trim();

// Corrige les imports faits avant la distinction remplacé / non publié
function fixEtat(x) {
  if (x.etat !== 'attente' && x.etat !== 'erreur') return x;
  if (isReplaced(x)) return { ...x, etat: 'remplace' };
  const age = x.date ? (Date.now() - new Date(x.date).getTime()) / 864e5 : 0;
  return age > 30 && x.etat === 'attente' ? { ...x, etat: 'absent' } : x;
}
export function loadMesDpe() {
  try { const l = JSON.parse(localStorage.getItem(KEY) || '[]'); return Array.isArray(l) ? l.filter(x => x && x.n).map(fixEtat) : []; } catch { return []; }
}
export function saveMesDpe(list) { try { localStorage.setItem(KEY, JSON.stringify(list)); return true; } catch { return false; } }

// ── Lecture du fichier ──
function parseCsv(text) {
  text = text.replace(/^﻿/, '');
  const first = text.split(/\r?\n/, 1)[0] || '';
  const sep = (first.match(/;/g) || []).length >= (first.match(/,/g) || []).length ? ';' : ',';
  const rows = []; let row = [], cell = '', q = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (q) { if (c === '"') { if (text[i + 1] === '"') { cell += '"'; i++; } else q = false; } else cell += c; }
    else if (c === '"') q = true;
    else if (c === sep) { row.push(cell); cell = ''; }
    else if (c === '\n') { row.push(cell); rows.push(row); row = []; cell = ''; }
    else if (c !== '\r') cell += c;
  }
  if (cell || row.length) { row.push(cell); rows.push(row); }
  return rows;
}
const toIso = v => {
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  const s = String(v ?? '').trim();
  let m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})/); if (m) return `${m[3]}-${m[2]}-${m[1]}`;
  m = s.match(/^(\d{4}-\d{2}-\d{2})/); return m ? m[1] : '';
};

// Renvoie [{ n, statut, date, commune }] (statut/date/commune seulement si le fichier les donne)
function fromRows(rows) {
  const head = (rows[0] || []).map(norm);
  const col = re => head.findIndex(h => re.test(h));
  const iNum = head.findIndex(h => /dpe/.test(h) && /(n°|^n |num|^no )/.test(h) && !/certificat|methode/.test(h));
  const iStat = col(/statut/), iDate = col(/etablissement/), iCom = col(/commune|ville/);
  const out = [];
  for (const r of rows.slice(1)) {
    let n = iNum >= 0 ? String(r[iNum] ?? '').trim() : '';
    if (!/^T?\d{4}[A-Z]\d{7}[A-Z]$/.test(n)) n = (r.map(String).join(' ').match(RE_NUM) || [])[0] || '';
    if (!n) continue;
    out.push({
      n,
      statut: iStat >= 0 ? String(r[iStat] ?? '').trim() : '',
      date: iDate >= 0 ? toIso(r[iDate]) : '',
      commune: iCom >= 0 ? String(r[iCom] ?? '').trim() : '',
    });
  }
  return out;
}

export async function readFile(file) {
  const name = file.name.toLowerCase();
  let items;
  if (name.endsWith('.xlsx') || name.endsWith('.xls')) {
    const { readSheet } = await import('read-excel-file/browser');
    const res = await readSheet(file);
    const rows = Array.isArray(res) ? res : (res.rows || res.data || res[0]?.data || []);
    items = fromRows(rows);
  } else {
    const text = await file.text();
    if (name.endsWith('.xml') || /^\s*</.test(text)) items = [...new Set(text.match(RE_NUM) || [])].map(n => ({ n, statut: '', date: '', commune: '' }));
    else items = fromRows(parseCsv(text));
  }
  const seen = new Set();
  return items.filter(x => !seen.has(x.n) && seen.add(x.n));
}

// ── Interrogation ADEME ──
async function ademe(dataset, nums) {
  const url = `${BASE}/${dataset}/lines?size=${nums.length}&select=${FIELDS}&numero_dpe_in=${nums.map(encodeURIComponent).join(',')}`;
  for (let i = 0; i < 3; i++) {
    if (i) await new Promise(ok => setTimeout(ok, 1000 * i));
    const r = await fetch(url);
    if (r.status === 429) continue;
    if (!r.ok) return null;
    return (await r.json()).results || [];
  }
  return null;
}
const NEUF = 'g3cgx7jb3cmys5voxz1mrm22';
const TERT = 'j9ol0fwjqckyf49vr29nknbu'; // DPE tertiaire (depuis juillet 2021) : n° du type 2618T0063102M
export const isTert = n => /^\d{4}T/.test(n || '');

// Complète chaque DPE importé. Un DPE absent de l'open data reste « en attente » (pas encore publié).
export async function enrich(items, onProgress) {
  const byNum = new Map(), ds_of = new Map();
  const nums = items.map(x => x.n);
  const todo = nums.slice();
  let done = 0, failed = false;
  for (const ds of ['dpe03existant', NEUF, TERT]) {
    const miss = ds === 'dpe03existant' ? todo : todo.filter(n => !byNum.has(n));
    for (let i = 0; i < miss.length; i += 30) {
      const res = await ademe(ds, miss.slice(i, i + 30));
      if (res === null) failed = true;
      for (const r of res || []) { byNum.set(r.numero_dpe, r); ds_of.set(r.numero_dpe, ds); }
      if (ds === 'dpe03existant') { done = Math.min(nums.length, i + 30); onProgress?.(done, nums.length); }
    }
  }
  const out = items.map(x => {
    const r = byNum.get(x.n);
    const base = { n: x.n, statut: x.statut || '', date: x.date || '', commune: x.commune || '' };
    if (!r) {
      if (failed) return { ...base, etat: 'erreur' };
      // Un DPE remplacé n'est plus publié en open data : ce n'est pas une attente
      if (isReplaced(x)) return { ...base, etat: 'remplace' };
      // L'ADEME publie avec une dizaine de jours de retard ; au-delà, le DPE n'est pas dans l'open data
      const age = x.date ? (Date.now() - new Date(x.date).getTime()) / 864e5 : 0;
      return { ...base, etat: age > 30 ? 'absent' : 'attente' };
    }
    const [lat, lng] = String(r._geopoint || '').split(',').map(Number);
    return {
      ...base,
      etat: 'ok', tert: ds_of.get(x.n) === TERT,
      date: r.date_etablissement_dpe || base.date,
      commune: r.nom_commune_ban || base.commune,
      cp: r.code_postal_ban || '', insee: r.code_insee_ban || '',
      etiquette: r.etiquette_dpe || '', ges: r.etiquette_ges || '',
      adresse: r.adresse_ban || '', brut: (r.adresse_brut || '').split('\n')[0],
      ban: r.identifiant_ban || '', score: r.score_ban ?? null,
      ref: r.complement_adresse_batiment || '',
      lat: Number.isFinite(lat) ? lat : null, lng: Number.isFinite(lng) ? lng : null,
    };
  });
  return out;
}

// Fusionne un nouvel import avec la liste existante (même n° = mis à jour)
export function merge(list, add) {
  const m = new Map(list.map(x => [x.n, x]));
  for (const x of add) m.set(x.n, { ...m.get(x.n), ...x });
  return [...m.values()].sort((a, b) => String(b.date).localeCompare(String(a.date)));
}


