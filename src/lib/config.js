// Constantes partagées : sources de données, couleurs, emprises.

export const FRANCE_BOUNDS = [-5.6, 41.2, 9.8, 51.2]; // France métropolitaine
export const MAX_BOUNDS = [-14, 35, 18, 57]; // assez large pour voir toute la France sur un téléphone

// Tuiles nationales (DPE, réseaux) sur Cloudflare R2
export const R2_BASE = 'https://pub-ba3b84e9a7d644e1854486f04e1f0116.r2.dev';
export const DPE_TILES_FALLBACK = '/public/dpe-44.pmtiles';

const WMTS = (layer, fmt = 'image/png', style = 'normal', matrix = 'PM') =>
  `https://data.geopf.fr/wmts?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0&LAYER=${layer}&STYLE=${style}&FORMAT=${encodeURIComponent(fmt)}&TILEMATRIXSET=${matrix}&TILEMATRIX={z}&TILEROW={y}&TILECOL={x}`;

export const BASEMAPS = {
  plan:  { label: 'Plan',      tiles: WMTS('GEOGRAPHICALGRIDSYSTEMS.PLANIGNV2'), attribution: '© IGN Géoplateforme' },
  ortho: { label: 'Satellite', tiles: WMTS('ORTHOIMAGERY.ORTHOPHOTOS', 'image/jpeg'), attribution: '© IGN Géoplateforme' },
  osm:   { label: 'OSM',       tiles: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png', attribution: '© OpenStreetMap contributors' },
};

// Photos aériennes anciennes (IGN)
export const HISTORIC = [
  { id: 'ORTHOIMAGERY.ORTHOPHOTOS.1950-1965', label: '1950-1965' },
  { id: 'ORTHOIMAGERY.ORTHOPHOTOS.1965-1980', label: '1965-1980' },
  { id: 'ORTHOIMAGERY.ORTHOPHOTOS.1980-1995', label: '1980-1995' },
];
export const historicTiles = id => WMTS(id, 'image/png', 'BDORTHOHISTORIQUE', 'PM_3_18');

export const CADASTRE_TILES = WMTS('CADASTRALPARCELS.PARCELLAIRE_EXPRESS');
export const IGN_WMS_V = 'https://data.geopf.fr/wms-v/ows?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&STYLES=&FORMAT=image/png&TRANSPARENT=true&CRS=EPSG:3857&WIDTH=256&HEIGHT=256&BBOX={bbox-epsg-3857}&LAYERS=';

// Couches proposées dans le menu « Couches » (sw : aperçu de l'aspect sur la carte)
export const OVERLAYS = [
  { id: 'dpe',      label: 'DPE',              sub: 'Couleur des bâtiments', group: 'Bâtiment',
    sw: 'linear-gradient(90deg,#009C6D 0 14%,#52B153 14% 28%,#78BD76 28% 42%,#F4E70F 42% 57%,#F0B50F 57% 71%,#EB8235 71% 85%,#D7221F 85%)' },
  { id: 'cadastre', label: 'Cadastre',         sub: 'Parcelles',             group: 'Bâtiment',
    sw: 'repeating-linear-gradient(0deg,transparent 0 8px,#7A5AA8 8px 9.5px),repeating-linear-gradient(90deg,#F6F2FB 0 10px,#7A5AA8 10px 11.5px)' },
  { id: 'rcu',      label: 'Chaleur',          sub: 'Réseaux de chaleur',    group: 'Réseaux',
    sw: 'linear-gradient(135deg,#FFF3EA 0 40%,#E05C00 40% 60%,#FFF3EA 60%)' },
  { id: 'gaz',      label: 'Gaz',              sub: 'Réseau GRDF',           group: 'Réseaux',
    sw: 'linear-gradient(135deg,#FFF8DC 0 40%,#D4A000 40% 60%,#FFF8DC 60%)' },
  { id: 'plu',      label: 'PLU',              sub: 'Zones d’urbanisme',     group: 'Urbanisme et patrimoine',
    sw: 'linear-gradient(135deg,#7C3AED 0 50%,#4ADE80 50%)' },
  { id: 'ac1',      label: 'Monuments',        sub: 'Abords des monuments', group: 'Urbanisme et patrimoine', sw: '#E8A3A3' },
  { id: 'ac4',      label: 'SPR',              sub: 'Sites patrimoniaux remarquables',   group: 'Urbanisme et patrimoine', sw: '#B48AD6' },
  { id: 'ac2',      label: 'Sites',            sub: 'Classés ou inscrits',               group: 'Urbanisme et patrimoine', sw: '#8FC79A' },
];

// Couleurs officielles des étiquettes DPE (pleines, jamais translucides dans l'interface)
export const DPE_HEX = { A: '#009C6D', B: '#52B153', C: '#78BD76', D: '#F4E70F', E: '#F0B50F', F: '#EB8235', G: '#D7221F' };
export const DPE_TEXT = { A: '#FFFFFF', B: '#FFFFFF', C: '#0F1A12', D: '#1A1700', E: '#1A1400', F: '#1A0D00', G: '#FFFFFF' };
export const GRADES = ['A', 'B', 'C', 'D', 'E', 'F', 'G'];

export const ADEME_DPE_URL = num => `https://observatoire-dpe-audit.ademe.fr/afficher-dpe/${num}`;
