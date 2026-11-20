// Constantes partagées : sources de données, couleurs, emprises.

export const FRANCE_BOUNDS = [-5.6, 41.2, 9.8, 51.2]; // France métropolitaine
export const MAX_BOUNDS = [-7.5, 40.0, 11.5, 52.5];

// Tuiles nationales (DPE, réseaux) sur Cloudflare R2
export const R2_BASE = 'https://pub-ba3b84e9a7d644e1854486f04e1f0116.r2.dev';
export const DPE_TILES_FALLBACK = '/public/dpe-44.pmtiles';

const WMTS = (layer, fmt = 'image/png', style = 'normal', matrix = 'PM') =>
  `https://data.geopf.fr/wmts?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0&LAYER=${layer}&STYLE=${style}&FORMAT=${encodeURIComponent(fmt)}&TILEMATRIXSET=${matrix}&TILEMATRIX={z}&TILEROW={y}&TILECOL={x}`;

// Fonds de carte : photos aériennes IGN, et plan vectoriel Protomaps (OpenStreetMap) hébergé sur R2
export const BASEMAPS = {
  ortho: { label: 'Satellite', tiles: WMTS('ORTHOIMAGERY.ORTHOPHOTOS', 'image/jpeg'), attribution: '© IGN Géoplateforme' },
  plan:  { label: 'Plan', vector: true },
};
export const PLAN_ATTRIBUTION = '<a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">© OpenStreetMap</a> · <a href="https://protomaps.com" target="_blank" rel="noopener">Protomaps</a>';
export const PLAN_ASSETS = 'https://protomaps.github.io/basemaps-assets';

// Photos aériennes anciennes (IGN)
export const HISTORIC = [
  { id: 'ORTHOIMAGERY.ORTHOPHOTOS.1950-1965', label: '1950-1965' },
  { id: 'ORTHOIMAGERY.ORTHOPHOTOS.1965-1980', label: '1965-1980' },
  { id: 'ORTHOIMAGERY.ORTHOPHOTOS.1980-1995', label: '1980-1995' },
];
export const historicTiles = id => WMTS(id, 'image/png', 'BDORTHOHISTORIQUE', 'PM_3_18');

export const CADASTRE_TILES = WMTS('CADASTRALPARCELS.PARCELLAIRE_EXPRESS');
export const IGN_WMS_V = 'https://data.geopf.fr/wms-v/ows?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&STYLES=&FORMAT=image/png&TRANSPARENT=true&CRS=EPSG:3857&WIDTH=256&HEIGHT=256&BBOX={bbox-epsg-3857}&LAYERS=';

// Couches proposées dans le menu « Couches » (logos : src/components/LayerIcon.jsx)
export const OVERLAYS = [
  { id: 'dpe',      label: 'Étiquettes DPE',                    group: 'Bâtiment' },
  { id: 'cadastre', label: 'Parcelles cadastrales',             group: 'Bâtiment' },
  { id: 'rcu',      label: 'Réseaux de chaleur',                group: 'Réseaux' },
  { id: 'gaz',      label: 'Réseau de gaz GRDF',                group: 'Réseaux' },
  { id: 'plu',      label: 'Zonage du PLU',                     group: 'Urbanisme et patrimoine' },
  { id: 'ac1',      label: 'Abords de monuments historiques',   group: 'Urbanisme et patrimoine' },
  { id: 'ac4',      label: 'Sites patrimoniaux remarquables',   group: 'Urbanisme et patrimoine' },
  { id: 'ac2',      label: 'Sites classés et inscrits',         group: 'Urbanisme et patrimoine' },
];

// Couleurs officielles des étiquettes DPE (pleines, jamais translucides dans l'interface)
export const DPE_HEX = { A: '#009C6D', B: '#52B153', C: '#78BD76', D: '#F4E70F', E: '#F0B50F', F: '#EB8235', G: '#D7221F' };
export const DPE_TEXT = { A: '#FFFFFF', B: '#FFFFFF', C: '#0F1A12', D: '#1A1700', E: '#1A1400', F: '#1A0D00', G: '#FFFFFF' };
export const GRADES = ['A', 'B', 'C', 'D', 'E', 'F', 'G'];

export const ADEME_DPE_URL = num => `https://observatoire-dpe-audit.ademe.fr/afficher-dpe/${num}`;
