// Mise en forme des données DPE et BDNB (reprise des règles de la version précédente).
import { GRADES } from './config.js';

const NUM = { A: 1, B: 2, C: 3, D: 4, E: 5, F: 6, G: 7 };
export const isGrade = g => !!NUM[g];
export const IMMEUBLE = 'dpe immeuble collectif';

export function shortEnergie(v) {
  if (!v) return null;
  return { 'Électricité': 'Électricité', 'Gaz naturel': 'Gaz', 'Réseau de chaleur urbain': 'Réseau de chaleur',
    'Fioul domestique': 'Fioul', 'Bois – Granulés (pellets) ou briquettes': 'Granulés', 'Bois – Bûches': 'Bois',
    'GPL': 'GPL', 'Charbon': 'Charbon', 'Propane': 'Propane', 'Autre': 'Autre' }[v] || v;
}
export function installType(v) {
  if (!v) return null;
  const s = v.toLowerCase();
  if (s.includes('collectif')) return 'collectif';
  if (s.includes('individuel')) return 'individuel';
  return null;
}
export function bdnbEnergie(v) {
  if (!v) return null;
  return { electricity: 'Électricité', natural_gas: 'Gaz', gaz: 'Gaz', fuel_oil: 'Fioul', fioul: 'Fioul', wood: 'Bois',
    bois: 'Bois', heat_network: 'Réseau de chaleur', reseau_chaleur: 'Réseau de chaleur', thermodynamic: 'Pompe à chaleur',
    pompe_a_chaleur: 'Pompe à chaleur', coal: 'Charbon', propane: 'GPL', gpl: 'GPL', other: 'Autre' }[v.toLowerCase()] || v;
}
export function usageLabel(v) {
  if (!v) return null;
  const s = v.toLowerCase();
  if (s.includes('individuel')) return 'Maison individuelle';
  if (s.includes('collectif')) return 'Logements collectifs';
  if (s.includes('tertiaire')) return 'Tertiaire';
  return v;
}
export function splitAddress(addr) {
  if (!addr) return ['', ''];
  const ci = addr.indexOf(',');
  if (ci > 0) return [addr.slice(0, ci).trim(), addr.slice(ci + 1).trim()];
  const m = addr.match(/^(.+?)\s{1,3}(\d{5}\s+.+)$/);
  return m ? [m[1].trim(), m[2].trim()] : [addr, ''];
}
export const frDate = d => (d && d.length >= 10 ? `${d.slice(8, 10)}/${d.slice(5, 7)}/${d.slice(0, 4)}` : '—');

const byDate = (a, b) => String(b.date_reception_dpe || '').localeCompare(String(a.date_reception_dpe || ''));

// Étiquette retenue pour le bâtiment : DPE immeuble, sinon DPE le plus récent d'une maison, sinon moyenne des logements
export function bestGrade(existants = [], neufs = []) {
  const imm = existants.find(r => r.methode_application_dpe === IMMEUBLE);
  if (isGrade(imm?.etiquette_dpe)) return imm.etiquette_dpe;
  const all = [...existants, ...neufs];
  if (all.length && all.every(r => r.methode_application_dpe === 'dpe maison individuelle' || !r.methode_application_dpe)) {
    const latest = all.filter(r => isGrade(r.etiquette_dpe)).sort(byDate)[0];
    if (latest) return latest.etiquette_dpe;
  }
  const nums = all.map(r => NUM[r.etiquette_dpe]).filter(Boolean);
  if (!nums.length) return null;
  return GRADES[Math.round(nums.reduce((a, b) => a + b, 0) / nums.length) - 1] || null;
}

// Comment l'étiquette du bâtiment est obtenue : un DPE précis (immeuble, maison, DPE unique)
// ou la moyenne de plusieurs DPE de logements. En moyenne : kWh et GES moyens, pas de date.
export function syntheseDPE(existants = [], neufs = []) {
  const all = [...existants, ...neufs];
  if (!all.length) return null;
  const imm = existants.find(r => r.methode_application_dpe === IMMEUBLE && isGrade(r.etiquette_dpe));
  if (imm) return { mode: 'immeuble', ref: imm, n: all.length };
  const maisons = all.every(r => r.methode_application_dpe === 'dpe maison individuelle' || !r.methode_application_dpe);
  const notes = all.filter(r => isGrade(r.etiquette_dpe));
  if (maisons || notes.length <= 1) return { mode: 'unique', ref: [...(notes.length ? notes : all)].sort(byDate)[0], n: all.length };
  const kwh = notes.map(r => Number(r.conso_5_usages_par_m2_ep)).filter(v => Number.isFinite(v) && v > 0);
  const ges = notes.map(r => NUM[r.etiquette_ges]).filter(Boolean);
  return {
    mode: 'moyenne', n: notes.length,
    kwh: kwh.length ? Math.round(kwh.reduce((a, b) => a + b, 0) / kwh.length) : null,
    ges: ges.length ? GRADES[Math.round(ges.reduce((a, b) => a + b, 0) / ges.length) - 1] : null,
  };
}

// DPE de référence pour la fiche (immeuble en priorité, sinon le plus récent)
export function referenceDPE(existants = [], neufs = []) {
  const all = [...existants, ...neufs];
  if (!all.length) return null;
  return all.find(r => r.methode_application_dpe === IMMEUBLE) || [...all].sort(byDate)[0];
}

export function summary(existants = [], neufs = []) {
  const all = [...existants, ...neufs];
  if (!all.length) return null;
  const ref = referenceDPE(existants, neufs);
  const years = all.map(r => r.annee_construction || r.periode_construction).filter(Boolean).map(String);
  const surfs = all.filter(r => r.methode_application_dpe !== IMMEUBLE)
    .map(r => parseFloat(r.surface_habitable_logement)).filter(v => v > 0);
  const mn = Math.round(Math.min(...surfs)), mx = Math.round(Math.max(...surfs));
  return {
    ref,
    chauffage: shortEnergie(ref.type_energie_principale_chauffage),
    chauffageType: installType(ref.type_installation_chauffage_n1),
    ecs: shortEnergie(ref.type_energie_principale_ecs),
    ecsType: installType(ref.type_installation_ecs_n1),
    year: years.length ? years.sort((a, b) => b.localeCompare(a))[0] : null,
    surface: !surfs.length ? null : mn === mx ? `${mn} m²` : `${mn} à ${mx} m²`,
  };
}

// Étiquette avant le changement du coefficient de l'électricité (1er janvier 2026), si elle diffère
export function preCoeffGrade(r) {
  const date = r.date_reception_dpe || '';
  if (date < '2021-07-01' || date >= '2026-01-01') return null;
  const ch = r.type_energie_principale_chauffage || '', ecs = r.type_energie_principale_ecs || '';
  if (!ch.includes('Électricité') && !ecs.includes('Électricité')) return null;
  const ep = parseFloat(r.conso_5_usages_par_m2_ep);
  if (!ep || ep <= 0) return null;
  const old = ep * (2.58 / 2.3);
  const i = [70, 110, 180, 250, 330, 420].findIndex(t => old <= t);
  const g = i === -1 ? 'G' : GRADES[i];
  return g !== r.etiquette_dpe ? g : null;
}

// Zone climatique du DPE (méthode 3CL) selon le département
const ZONES = {
  H1a: '02 14 27 59 60 62 75 76 77 78 80 91 92 93 94 95',
  H1b: '08 10 45 51 52 54 55 57 58 67 68 70 88 89 90',
  H1c: '01 03 05 15 19 21 23 25 38 39 42 43 63 69 71 73 74 87',
  H2a: '22 29 35 50 56',
  H2b: '16 17 18 28 36 37 41 44 49 53 61 72 79 85 86',
  H2c: '09 12 24 31 32 33 40 46 47 64 65 81 82',
  H2d: '04 07 26 48 84',
  H3: '06 11 13 2A 2B 30 34 66 83',
};
export function zoneClimatique(codeInsee) {
  const c = String(codeInsee || '').toUpperCase();
  if (c.length < 5 || c.startsWith('97')) return null;
  const dep = c.slice(0, 2);
  return Object.keys(ZONES).find(z => ZONES[z].split(' ').includes(dep)) || null;
}
export function classeAltitude(z) {
  if (z == null) return null;
  return z < 400 ? 'inférieure à 400 m' : z <= 800 ? 'de 400 à 800 m' : 'supérieure à 800 m';
}
