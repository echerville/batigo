// Carte privée : repères posés par l'utilisateur (point ou zone), enregistrés sur son appareil.
// Version sans compte : tout reste dans le navigateur (localStorage). Le format est prêt
// pour un passage vers la base de données quand les comptes existeront.
const KEY = 'scope.reperes.v1';

// Icônes au trait (24×24) : contenu SVG constant, jamais issu de la saisie
const P = {
  pin: '<path d="M12 21s7-6.2 7-11a7 7 0 1 0-14 0c0 4.8 7 11 7 11Z"/><circle cx="12" cy="10" r="2.5"/>',
  flag: '<path d="M5 21V4M5 4h11l-2 4 2 4H5"/>',
  alert: '<path d="M12 3 2 20h20L12 3Z"/><path d="M12 10v4M12 17h.01"/>',
  note: '<path d="M5 4h14v12l-4 4H5V4Z"/><path d="M15 20v-4h4M8 9h8M8 13h5"/>',
  camera: '<path d="M4 8h3l2-3h6l2 3h3v11H4z"/><circle cx="12" cy="13" r="3.5"/>',
  key: '<circle cx="8" cy="15" r="4"/><path d="m11 12 9-9M16 7l3 3"/>',
  flame: '<path d="M12 3c1 4 5 5.5 5 10a5 5 0 0 1-10 0c0-2 1-3 2-4 0 2 1 3 2 3 0-3-1-6 1-9Z"/>',
  bolt: '<path d="M13 2 4 14h7l-1 8 9-12h-7l1-8Z"/>',
  drop: '<path d="M12 3s6 6.5 6 11a6 6 0 0 1-12 0c0-4.5 6-11 6-11Z"/>',
  house: '<path d="M3 11 12 4l9 7M5 10v10h14V10M10 20v-6h4v6"/>',
  window: '<rect x="4" y="4" width="16" height="16" rx="1"/><path d="M12 4v16M4 12h16"/>',
  stairs: '<path d="M4 20h5v-5h5v-5h6"/>',
  termite: '<ellipse cx="12" cy="14" rx="4" ry="5.5"/><path d="M12 8.5V4M9 4l3 2 3-2M8 12H4M8 16l-4 2M16 12h4M16 16l4 2"/>',
  insect: '<ellipse cx="12" cy="13" rx="3" ry="6"/><path d="M9.5 9C6 4 3 5 3 8s3 4 6.5 2.5M14.5 9C18 4 21 5 21 8s-3 4-6.5 2.5M9 14h6M9.5 17h5"/>',
  mushroom: '<path d="M3 12a9 8 0 0 1 18 0H3Z"/><path d="M9 12v7a1 1 0 0 0 1 1h4a1 1 0 0 0 1-1v-7"/>',
  plaque: '<path d="M7 5h14l-4 14H3L7 5Z"/><path d="M10.5 5 6.5 19M14 5l-4 14M17.5 5l-4 14"/>',
  digicode: '<rect x="5" y="2.5" width="14" height="19" rx="2"/><path d="M9 7h.01M12 7h.01M15 7h.01M9 11h.01M12 11h.01M15 11h.01M9 15h.01M12 15h.01M15 15h.01M12 18.5h.01"/>',
  door: '<path d="M6 21V4h12v17M4 21h16"/><path d="M15 12.5h.01"/>',
  star: '<path d="m12 3 2.8 5.8 6.2.9-4.5 4.4 1.1 6.2L12 17.3l-5.6 3 1.1-6.2L3 9.7l6.2-.9L12 3Z"/>',
};
// Bibliothèque à plat : l'utilisateur choisit lui-même le logo, la couleur et le nom
export const REPERE_ICONS = [
  ['pin', 'Repère'], ['flag', 'Drapeau'], ['star', 'Étoile'], ['alert', 'Attention'], ['note', 'Note'], ['camera', 'Photo'],
  ['key', 'Clé'], ['digicode', 'Digicode'], ['door', 'Porte'], ['stairs', 'Escalier'], ['window', 'Fenêtre'], ['house', 'Toiture'],
  ['flame', 'Chaudière'], ['bolt', 'Électricité'], ['drop', 'Eau'], ['plaque', 'Plaque ondulée'],
  ['termite', 'Termites'], ['insect', 'Insectes'], ['mushroom', 'Champignons'],
];
export const REPERE_COLORS = ['#17804D', '#2B5BA8', '#C0392B', '#E05C00', '#F0B50F', '#7B2FBE', '#0E7C86', '#D6336C', '#5C6370', '#1F2430'];
export const DEFAULT_COLOR = REPERE_COLORS[0];
export const iconSvg = (id, size = 20, stroke = 'currentColor') =>
  `<svg width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="${stroke}" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${P[id] || P.pin}</svg>`;

// Pas de nom de client ni de coordonnées dans les notes (RGPD)
const RE_MAIL = /\S+@\S+\.\S+/;
const RE_TEL = /(?:\+33|\b0)\s?[1-9](?:[\s.-]?\d{2}){4}\b/;
export function checkText(t) {
  if (RE_MAIL.test(t) || RE_TEL.test(t)) return 'Pas d’adresse e-mail ni de numéro de téléphone dans les notes.';
  return null;
}

export function loadReperes() {
  try {
    const l = JSON.parse(localStorage.getItem(KEY) || '[]');
    return Array.isArray(l) ? l.filter(r => r && r.id && r.coords) : [];
  } catch { return []; }
}
export function saveReperes(list) {
  try { localStorage.setItem(KEY, JSON.stringify(list)); return true; } catch { return false; }
}
export const newId = () => (crypto.randomUUID ? crypto.randomUUID() : `r${Date.now()}${Math.random().toString(36).slice(2, 7)}`);
export function exportReperes(list) {
  const blob = new Blob([JSON.stringify({ app: 'scope', version: 1, reperes: list }, null, 2)], { type: 'application/json' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob); a.download = `scope-reperes-${new Date().toISOString().slice(0, 10)}.json`;
  document.body.appendChild(a); a.click(); a.remove(); setTimeout(() => URL.revokeObjectURL(a.href), 2000);
}

// Modèles : combinaisons nom + logo + couleur que l'utilisateur veut reposer souvent
const KEY_M = 'scope.repmodeles.v1';
export function loadModeles() {
  try { const l = JSON.parse(localStorage.getItem(KEY_M) || '[]'); return Array.isArray(l) ? l.filter(m => m && m.id) : []; } catch { return []; }
}
export function saveModeles(list) { try { localStorage.setItem(KEY_M, JSON.stringify(list)); } catch { /* silencieux */ } }

// Repères à afficher dans la fiche d'un bien. Repère « propre au bien » : seulement s'il est sur ce
// bâtiment (même identifiant BDNB), ou à moins de 15 m s'il n'a pas pu être rattaché à un bâtiment.
// Autres repères (termites, mérule…) : points à moins de `rayon` mètres, zones qui contiennent le point
// ou dont un sommet est à moins de `rayon` mètres. Triés par distance.
const distM = ([lng1, lat1], [lng2, lat2]) => {
  const k = Math.PI / 180, x = (lng2 - lng1) * k * Math.cos(((lat1 + lat2) / 2) * k), y = (lat2 - lat1) * k;
  return Math.hypot(x, y) * 6371000;
};
const dansZone = ([x, y], ring) => {
  let ins = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i], [xj, yj] = ring[j];
    if ((yi > y) !== (yj > y) && x < (xj - xi) * (y - yi) / (yj - yi) + xi) ins = !ins;
  }
  return ins;
};
export function reperesProches(list, ll, batId = null, rayon = 40) {
  if (!ll) return [];
  return list.map(r => {
    if (r.type === 'zone' && Array.isArray(r.coords) && r.coords.length > 2) {
      if (dansZone(ll, r.coords)) return { r, d: 0 };
      return { r, d: Math.min(...r.coords.map(c => distM(ll, c))) };
    }
    return { r, d: Array.isArray(r.coords) ? distM(ll, r.coords) : Infinity };
  }).filter(({ r, d }) => {
    if (!r.exclusif) return d <= rayon;
    if (r.batId) return !!batId && r.batId === batId;
    return d <= 15;
  }).sort((a, b) => a.d - b.d);
}
