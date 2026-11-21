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
  amiante: '<path d="M12 3 2 20h20L12 3Z"/><path d="m9.2 17 2.8-7 2.8 7M10.2 14.8h3.6"/>',
};
export const REPERE_GROUPS = [
  { id: 'standard', label: 'Standard', color: '#17804D', icons: [['pin', 'Repère'], ['flag', 'Important'], ['alert', 'Attention'], ['note', 'Note'], ['camera', 'Photo'], ['key', 'Accès']] },
  { id: 'batiment', label: 'Éléments du bâtiment', color: '#2B5BA8', icons: [['flame', 'Chaudière'], ['bolt', 'Électricité'], ['drop', 'Eau'], ['house', 'Toiture'], ['window', 'Ouvrant'], ['stairs', 'Escalier']] },
  { id: 'diag', label: 'Termites, insectes, champignons, amiante', color: '#C0392B', icons: [['termite', 'Termites'], ['insect', 'Insectes'], ['mushroom', 'Champignons'], ['amiante', 'Amiante']] },
];
const ICON_GROUP = Object.fromEntries(REPERE_GROUPS.flatMap(g => g.icons.map(([id]) => [id, g])));
export const iconColor = id => (ICON_GROUP[id] || REPERE_GROUPS[0]).color;
export const iconLabel = id => REPERE_GROUPS.flatMap(g => g.icons).find(([i]) => i === id)?.[1] || 'Repère';
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
