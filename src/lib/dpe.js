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
