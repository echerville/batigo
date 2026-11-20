// Carte MapLibre : fonds, couches, sélection du bâtiment, mesure.
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { Protocol } from 'pmtiles';
import { layers as pmLayers, namedFlavor } from '@protomaps/basemaps';
import centroid from '@turf/centroid';
import booleanPointInPolygon from '@turf/boolean-point-in-polygon';
import { point } from '@turf/helpers';
import {
  BASEMAPS, FRANCE_BOUNDS, MAX_BOUNDS, CADASTRE_TILES, IGN_WMS_V, R2_BASE,
  DPE_TILES_FALLBACK, DPE_HEX, historicTiles, PLAN_ATTRIBUTION, PLAN_ASSETS,
} from './config.js';
import { r2Meta, fetchPLUBounds } from './data.js';
import { bestGrade } from './dpe.js';

const protocol = new Protocol();
maplibregl.addProtocol('pmtiles', protocol.tile.bind(protocol));

const DPE_COLOR = ['match', ['get', 'dpe'], ...Object.entries(DPE_HEX).flat(), '#AAAAAA'];
const PLU_COLOR = ['case', ['==', ['get', 'typezone'], 'U'], '#7C3AED', ['==', ['get', 'typezone'], 'AU'], '#A78BFA',
  ['==', ['get', 'typezone'], 'A'], '#4ADE80', ['==', ['get', 'typezone'], 'N'], '#166534', '#94A3B8'];
const EMPTY = { type: 'FeatureCollection', features: [] };
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

// Vue de départ : la France métropolitaine entière, un peu resserrée
function franceView(el, mobile) {
  const [w0, s0, e0, n0] = mobile ? [-4.9, 42.3, 8.3, 51.1] : [-5.0, 42.0, 8.5, 51.1];
  const W = el.clientWidth || window.innerWidth, H = (el.clientHeight || window.innerHeight) - (mobile ? 170 : 40);
  const my = lat => (1 - Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI) / 360)) / Math.PI) / 2;
  const zx = Math.log2((W * 360) / (512 * (e0 - w0)));
  const zy = Math.log2(H / (512 * (my(s0) - my(n0))));
  return { center: [(w0 + e0) / 2 + (mobile ? 0 : 0.6), mobile ? 46.2 : 46.6], zoom: Math.min(zx, zy) };
}

export function createMap(container, { onPick, onStatus, mobile }) {
  const map = new maplibregl.Map({
    container,
    style: {
      version: 8,
      glyphs: `${PLAN_ASSETS}/fonts/{fontstack}/{range}.pbf`,
      sprite: `${PLAN_ASSETS}/sprites/v4/light`,
      sources: { base: { type: 'raster', tiles: [BASEMAPS.ortho.tiles], tileSize: 256, maxzoom: 19, attribution: BASEMAPS.ortho.attribution } },
      layers: [{ id: 'base-layer', type: 'raster', source: 'base' }],
    },
    // Démarrage sur la France entière (le fond IGN couvre aussi les pays voisins aux petits zooms : pas de bord blanc)
    ...franceView(container, mobile), minZoom: 4, maxBounds: MAX_BOUNDS,
    attributionControl: false,
  });
  // Mobile : en haut, pour ne pas passer sous la fiche glissante
  map.addControl(new maplibregl.AttributionControl({ compact: true }), mobile ? 'top-left' : 'bottom-right');
  map.addControl(new maplibregl.ScaleControl({ unit: 'metric' }), mobile ? 'top-left' : 'bottom-left');

  const state = {
    overlays: {}, loaded: {}, measure: null, selBatId: null, corrections: new Map(),
    addrMarker: null, measureMarkers: [], basemap: 'ortho', historic: null, dpeTiles: DPE_TILES_FALLBACK,
  };
  const ready = new Promise(res => map.once('load', res));
  let rcuPopup;

  map.on('load', () => {
    map.addSource('cadastre', { type: 'raster', tiles: [CADASTRE_TILES], tileSize: 256, maxzoom: 19, bounds: FRANCE_BOUNDS, attribution: '© IGN Cadastre' });
    map.addLayer({ id: 'cadastre-layer', type: 'raster', source: 'cadastre', layout: { visibility: 'none' }, paint: { 'raster-opacity': 0.7 } });

    map.addSource('dpe-src', { type: 'vector', url: `pmtiles://${location.origin}${DPE_TILES_FALLBACK}` });
    map.addLayer({ id: 'dpe-fill', type: 'fill', source: 'dpe-src', 'source-layer': 'dpe', minzoom: 13, layout: { visibility: 'none' }, paint: { 'fill-color': DPE_COLOR, 'fill-opacity': 0.6 } });
    map.addLayer({ id: 'dpe-line', type: 'line', source: 'dpe-src', 'source-layer': 'dpe', minzoom: 13, layout: { visibility: 'none' }, paint: { 'line-color': DPE_COLOR, 'line-width': 1, 'line-opacity': 0.9 } });
    // Couche invisible toujours active : sert à retrouver les contours des bâtiments
    map.addLayer({ id: 'dpe-query', type: 'fill', source: 'dpe-src', 'source-layer': 'dpe', minzoom: 13, paint: { 'fill-opacity': 0 } });

    // Hachures des bâtiments neufs
    const n = 8, data = new Uint8Array(n * n * 4);
    for (let y = 0; y < n; y++) for (let x = 0; x < n; x++) if ((x + y) % n < 2) data.set([255, 255, 255, 170], (y * n + x) * 4);
    map.addImage('hatch-pat', { width: n, height: n, data });
    map.addLayer({ id: 'dpe-neuf-hatch', type: 'fill', source: 'dpe-src', 'source-layer': 'dpe', minzoom: 13, layout: { visibility: 'none' }, filter: ['==', ['get', 'neuf'], 1], paint: { 'fill-pattern': 'hatch-pat' } });

    // Couleur ADEME vérifiée des bâtiments consultés + contour du bâtiment sélectionné
    map.addSource('sel-dpe-src', { type: 'geojson', data: EMPTY });
    map.addLayer({ id: 'sel-dpe-fill', type: 'fill', source: 'sel-dpe-src', paint: { 'fill-color': ['get', 'color'], 'fill-opacity': 0.85 } });
    map.addSource('neuf-hatch-src', { type: 'geojson', data: EMPTY });
    map.addLayer({ id: 'neuf-hatch-fill', type: 'fill', source: 'neuf-hatch-src', paint: { 'fill-pattern': 'hatch-pat' } });
    map.addSource('sel-outline-src', { type: 'geojson', data: EMPTY });
    map.addLayer({ id: 'sel-halo', type: 'line', source: 'sel-outline-src', paint: { 'line-color': '#FFFFFF', 'line-width': 6, 'line-opacity': 0.9 } });
    map.addLayer({ id: 'sel-outline', type: 'line', source: 'sel-outline-src', paint: { 'line-color': '#121826', 'line-width': 3 } });

    // Urbanisme et patrimoine
    map.addSource('plu-src', { type: 'geojson', data: EMPTY });
    map.addLayer({ id: 'plu-fill', type: 'fill', source: 'plu-src', layout: { visibility: 'none' }, paint: { 'fill-color': PLU_COLOR, 'fill-opacity': 0.18 } });
    map.addLayer({ id: 'plu-line', type: 'line', source: 'plu-src', layout: { visibility: 'none' }, paint: { 'line-color': PLU_COLOR, 'line-width': 1.5, 'line-opacity': 0.8 } });
    for (const [id, layer] of [['ac1', 'monument_historique'], ['ac4', 'zone_secteur'], ['ac2', 'monument_naturel_site']]) {
      map.addSource(`${id}-src`, { type: 'raster', tiles: [IGN_WMS_V + layer], tileSize: 256 });
      map.addLayer({ id: `${id}-layer`, type: 'raster', source: `${id}-src`, layout: { visibility: 'none' }, paint: { 'raster-opacity': 0.8 } });
    }

    // Mesure
    map.addSource('measure-src', { type: 'geojson', data: EMPTY });
    map.addLayer({ id: 'measure-fill', type: 'fill', source: 'measure-src', paint: { 'fill-color': '#2BB573', 'fill-opacity': 0.2 }, filter: ['==', '$type', 'Polygon'] });
    map.addLayer({ id: 'measure-line', type: 'line', source: 'measure-src', paint: { 'line-color': '#17804D', 'line-width': 3 } });

    // Info-bulles
    const hover = new maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 6, className: 'scope-tip' });
    map.on('mousemove', 'dpe-fill', e => {
      const p = e.features[0].properties;
      hover.setLngLat(e.lngLat).setHTML(`<b>${esc(p.dpe || 'Sans DPE')}</b>${p.nb_log > 0 ? ` · ${esc(p.nb_log)} logements` : ''}`).addTo(map);
    });
    map.on('mouseleave', 'dpe-fill', () => hover.remove());
    map.on('mousemove', 'plu-fill', e => {
      const p = e.features[0].properties;
      hover.setLngLat(e.lngLat).setHTML(`<b>${esc(p.libelle || '?')}</b>${p.libelong ? ' · ' + esc(p.libelong) : ''}`).addTo(map);
    });
    map.on('mouseleave', 'plu-fill', () => hover.remove());
    rcuPopup = new maplibregl.Popup({ closeButton: true, closeOnClick: true, maxWidth: '300px', offset: 8, className: 'scope-pop' });
  });

  // Tuiles DPE nationales (R2), en remplacement du fichier Loire-Atlantique
  r2Meta('dpe-france').then(meta => {
    if (!meta?.fichier) return;
    state.dpeTiles = `${R2_BASE}/${meta.fichier}`;
    state.dpeMeta = meta;
    ready.then(() => {
      const src = map.getSource('dpe-src');
      const go = () => src.setUrl(`pmtiles://${state.dpeTiles}`);
      if (src.loaded()) return go();
      // Attendre la réponse de la source initiale, sinon elle remet l'emprise Loire-Atlantique
      const h = e => { if (e.sourceId === 'dpe-src' && e.sourceDataType === 'metadata') { map.off('sourcedata', h); go(); } };
      map.on('sourcedata', h);
    });
  }).catch(() => {});

  // ── Clic sur la carte : mesure, ou sélection du bâtiment qui contient le point ──
  map.on('click', e => {
    if (state.measure) { addMeasurePoint([e.lngLat.lng, e.lngLat.lat]); return; }
    if (map.queryRenderedFeatures(e.point, { layers: ['rcu-layer', 'rcu-points'].filter(l => map.getLayer(l)) }).length) return;
    const { lng, lat } = e.lngLat;
    const feats = map.queryRenderedFeatures([[e.point.x - 8, e.point.y - 8], [e.point.x + 8, e.point.y + 8]], { layers: ['dpe-query'] })
      .filter(f => { try { return booleanPointInPolygon(point([lng, lat]), f); } catch { return false; } });
    let at = [lng, lat], bldGroupId = null, tileGrade = null;
    if (feats.length) {
      try { at = centroid(feats[0]).geometry.coordinates; } catch { /* point de clic */ }
      bldGroupId = feats[0].properties?.id || null;
      tileGrade = feats[0].properties?.dpe || null;
    }
    onPick?.({ geocodeAt: at, clickAt: [lng, lat], bldGroupId, tileGrade });
  });
  map.on('dblclick', e => {
    if (!state.measure) return;
    e.preventDefault();
    if (state.measure.points.length >= 3) api.stopMeasure(true);
  });

  // ── Mesure ──
  function addMeasurePoint(pt) {
    const m = state.measure;
    m.points.push(pt);
    const el = document.createElement('div');
    el.className = 'measure-dot';
    state.measureMarkers.push(new maplibregl.Marker({ element: el }).setLngLat(pt).addTo(map));
    drawMeasure();
  }
  function drawMeasure() {
    const pts = state.measure?.points || [];
    const src = map.getSource('measure-src');
    if (!src) return;
    let geom = null;
    if (pts.length >= 3) geom = { type: 'Polygon', coordinates: [[...pts, pts[0]]] };
    else if (pts.length === 2) geom = { type: 'LineString', coordinates: pts };
    src.setData(geom ? { type: 'FeatureCollection', features: [{ type: 'Feature', geometry: geom, properties: {} }] } : EMPTY);
    state.measure?.onChange?.(pts.slice());
  }

  // ── Couches activables ──
  async function loadRcu() {
    const m = await r2Meta('rcu');
    const pts = await fetch(`${R2_BASE}/${m.points}`).then(r => { if (!r.ok) throw new Error(); return r.json(); });
    map.addSource('rcu-src', { type: 'vector', url: `pmtiles://${R2_BASE}/${m.fichier}` });
    map.addLayer({ id: 'rcu-layer', type: 'line', source: 'rcu-src', 'source-layer': 'rcu',
      paint: { 'line-color': '#E05C00', 'line-width': ['interpolate', ['linear'], ['zoom'], 8, 1, 12, 2, 16, 4], 'line-opacity': 0.85 } }, 'measure-fill');
    map.addSource('rcu-pts', { type: 'geojson', data: pts });
    map.addLayer({ id: 'rcu-points', type: 'circle', source: 'rcu-pts',
      paint: { 'circle-radius': ['interpolate', ['linear'], ['zoom'], 5, 3, 10, 6, 14, 8],
        'circle-color': ['case', ['==', ['get', 'trace'], 1], '#E05C00', '#FFFFFF'],
        'circle-stroke-color': ['case', ['==', ['get', 'trace'], 1], '#FFFFFF', '#E05C00'], 'circle-stroke-width': 2 } }, 'measure-fill');
    for (const id of ['rcu-layer', 'rcu-points']) {
      map.on('mouseenter', id, () => { map.getCanvas().style.cursor = 'pointer'; });
      map.on('mouseleave', id, () => { map.getCanvas().style.cursor = ''; });
      map.on('click', id, e => {
        const p = e.features[0].properties;
        rcuPopup.setLngLat(e.lngLat).setHTML(`<div class="pop-title">${esc((p.nom || 'Réseau de chaleur').trim())}</div>
          <dl class="pop-grid"><dt>Gestionnaire</dt><dd>${esc(p.gestionnaire || '—')}</dd>
          <dt>Taux EnR&amp;R</dt><dd>${esc(p.enr || '—')}</dd><dt>Contenu CO₂</dt><dd>${esc(p.co2 || '—')}</dd></dl>
          ${String(p.trace) === '0' ? '<div class="pop-note">Tracé non publié : position approximative</div>' : ''}`).addTo(map);
      });
    }
    return pts.features.length;
  }
  async function loadGaz() {
    const m = await r2Meta('gaz');
    map.addSource('gaz-src', { type: 'vector', url: `pmtiles://${R2_BASE}/${m.fichier}` });
    map.addLayer({ id: 'gaz-layer', type: 'line', source: 'gaz-src', 'source-layer': 'gaz', minzoom: 12,
      paint: { 'line-color': ['match', ['downcase', ['to-string', ['get', 'propane']]], ['oui', 'true', '1', 'o'], '#8B5CF6', '#D4A000'],
        'line-width': ['interpolate', ['linear'], ['zoom'], 12, 1.5, 16, 3.5], 'line-opacity': 0.9 } }, 'measure-fill');
  }
  // Fond « Plan » : tuiles vectorielles Protomaps (extrait France sur R2), style clair
  let planIds = [];
  async function loadPlan() {
    if (planIds.length) return;
    const m = await r2Meta('fond');
    map.addSource('fond', { type: 'vector', url: `pmtiles://${R2_BASE}/${m.fichier}`, attribution: PLAN_ATTRIBUTION });
    const below = map.getLayer('cadastre-layer') ? 'cadastre-layer' : undefined;
    const add = (l) => {
      const id = `pm-${l.id}`;
      map.addLayer({ ...l, id, layout: { ...(l.layout || {}), visibility: 'none' } }, below);
      planIds.push(id);
    };
    // Bâtiments : BD TOPO de l'IGN (même source que la BDNB, donc calés sur les couleurs DPE)
    // plutôt que ceux d'OpenStreetMap, décalés de quelques mètres.
    map.addSource('ign-bati', { type: 'vector', tiles: ['https://data.geopf.fr/tms/1.0.0/PLAN.IGN/{z}/{x}/{y}.pbf'],
      maxzoom: 18, attribution: '© IGN' });
    const BATI = ['match', ['get', 'symbo'], ['BATI_QQUE', 'BATI_COMMERCIAL', 'BATI_INDUSTRIEL', 'HANGAR', 'HANGAR_COMMERCIAL',
      'HANGAR_INDUSTRIEL', 'HANGAR_PUBLIC', 'BATI_PUBLIC', 'BATI_GARE', 'BATI_SPORTIF', 'MAIRIE', 'MAIRIE_ANNEXE',
      'AUTRE_CULTE_SURF', 'CHAPELLE_SURF', 'CHRETIEN_SURF', 'EGLISE_SURF', 'MOSQUEE_SURF', 'RELIGIEUX_QQUE', 'SYNAGOGUE_SURF',
      'ARC_TRIOMPHE_SURF', 'ARENE_THEATRE', 'CHATEAU_SURF', 'FORTIF_SURF', 'MONUMENT_SURF', 'TOUR_MOULIN_SURF',
      'SILO_SURF', 'RESERVOIR_INDUS_SURF', 'SERRE_SURF', 'CHATEAU_EAU_SURF'], true, false];
    for (const l of pmLayers('fond', namedFlavor('light'), { lang: 'fr' })) {
      if (l.id === 'pois') continue; // pas de commerces ni de lieux
      if (l.id === 'buildings') {
        add({ id: 'bati-fill', type: 'fill', source: 'ign-bati', 'source-layer': 'bati_surf', filter: BATI, minzoom: 14,
          paint: { 'fill-color': '#d4d4d4', 'fill-opacity': 0.7 } });
        // Contour : sépare les bâtiments mitoyens
        add({ id: 'bati-line', type: 'line', source: 'ign-bati', 'source-layer': 'bati_surf', filter: BATI, minzoom: 14,
          paint: { 'line-color': '#8f8f8f', 'line-width': ['interpolate', ['linear'], ['zoom'], 14, 0.3, 17, 0.8, 20, 1.4] } });
        continue;
      }
      add(l);
    }
  }
  function showPlan(on) {
    for (const id of planIds) map.setLayoutProperty(id, 'visibility', on ? 'visible' : 'none');
    map.setLayoutProperty('base-layer', 'visibility', on ? 'none' : 'visible');
  }

  const LAYERS = {
    dpe: ['dpe-fill', 'dpe-line', 'dpe-neuf-hatch'], cadastre: ['cadastre-layer'], rcu: ['rcu-layer', 'rcu-points'],
    gaz: ['gaz-layer'], plu: ['plu-fill', 'plu-line'], ac1: ['ac1-layer'], ac4: ['ac4-layer'], ac2: ['ac2-layer'],
  };
  async function refreshPLU() {
    if (!state.overlays.plu || map.getZoom() < 12) return;
    try { map.getSource('plu-src').setData(await fetchPLUBounds(map.getBounds())); } catch { /* silencieux */ }
  }
  map.on('moveend', refreshPLU);

  const api = {
    map,
    ready,
    meta: () => state.dpeMeta,

    async setOverlay(id, on) {
      await ready;
      state.overlays[id] = on;
      try {
        if (on && id === 'rcu' && !state.loaded.rcu) {
          onStatus?.('Chargement des réseaux de chaleur…');
          const n = await loadRcu(); state.loaded.rcu = true;
          onStatus?.(`${n.toLocaleString('fr-FR')} réseaux de chaleur en France`, 3000);
        }
        if (on && id === 'gaz' && !state.loaded.gaz) { onStatus?.('Chargement du réseau de gaz…'); await loadGaz(); state.loaded.gaz = true; onStatus?.(null); }
      } catch {
        state.overlays[id] = false;
        onStatus?.('Couche indisponible pour le moment', 3000);
        return false;
      }
      for (const l of LAYERS[id]) if (map.getLayer(l)) map.setLayoutProperty(l, 'visibility', on ? 'visible' : 'none');
      if (on && id === 'gaz' && map.getZoom() < 12) onStatus?.('Réseau de gaz : zoomez pour l’afficher', 3000);
      if (on && id === 'plu') { if (map.getZoom() < 12) map.flyTo({ zoom: 13 }); else refreshPLU(); }
      return true;
    },

    async setBasemap(key) {
      await ready;
      state.historic = null;
      if (BASEMAPS[key].vector) {
        try { await loadPlan(); } catch { onStatus?.('Fond Plan indisponible pour le moment', 3000); return false; }
        state.basemap = key; showPlan(true);
      } else {
        state.basemap = key; showPlan(false);
        map.getSource('base')?.setTiles([BASEMAPS[key].tiles]);
      }
      return true;
    },
    setHistoric(id) {
      state.historic = id;
      if (id) { showPlan(false); map.getSource('base')?.setTiles([historicTiles(id)]); }
      else if (BASEMAPS[state.basemap].vector) showPlan(true);
      else map.getSource('base')?.setTiles([BASEMAPS[state.basemap].tiles]);
    },

    // Centre le bien dans la partie visible de la carte (à côté de la fiche, ou au-dessus sur mobile)
    flyTo(lngLat, zoom = 18) {
      const padding = mobile ? { top: 70, bottom: Math.round(window.innerHeight * 0.5), left: 0, right: 0 } : { left: 436, top: 0, right: 0, bottom: 0 };
      map.flyTo({ center: lngLat, zoom, speed: 1.4, padding });
    },

    placeMarker(lngLat) {
      state.addrMarker?.remove();
      const el = document.createElement('div');
      el.className = 'addr-marker';
      el.innerHTML = '<svg width="26" height="34" viewBox="0 0 22 30" aria-hidden="true"><path d="M11 0C4.9 0 0 4.9 0 11c0 7.7 11 19 11 19s11-11.3 11-19C22 4.9 17.1 0 11 0z" fill="#17804D" stroke="#fff" stroke-width="1.5"/><circle cx="11" cy="11" r="4.5" fill="#fff"/></svg>';
      state.addrMarker = new maplibregl.Marker({ element: el, anchor: 'bottom' }).setLngLat(lngLat).addTo(map);
    },

    // Bâtiment sélectionné : contour noir + couleur du DPE ADEME vérifié (gardée pendant la session)
    select(batId) {
      state.selBatId = batId;
      api.applySelection(null);
    },
    applySelection(dpe, retry = 0) {
      const sel = map.getSource('sel-dpe-src'), hatch = map.getSource('neuf-hatch-src'), outline = map.getSource('sel-outline-src');
      if (!sel || !hatch || !outline) { ready.then(() => api.applySelection(dpe, retry)); return; }
      const render = () => sel.setData({ type: 'FeatureCollection', features: [...state.corrections.values()].flat() });
      const id = state.selBatId;
      const feats = id ? map.queryRenderedFeatures({ layers: ['dpe-query'] }).filter(f => f.properties.id === id) : [];
      if (!feats.length) {
        outline.setData(EMPTY); hatch.setData(EMPTY); render();
        if (id && retry < 4) map.once('idle', () => { if (state.selBatId === id) api.applySelection(dpe, retry + 1); });
        return null;
      }
      const plainGeom = feats.map(f => ({ type: 'Feature', geometry: f.geometry, properties: {} }));
      outline.setData({ type: 'FeatureCollection', features: plainGeom });
      const tileGrade = feats[0].properties?.dpe || null;
      if (dpe) {
        const has = dpe.existants.length || dpe.neufs.length;
        if (!has) state.corrections.delete(id);
        else {
          const color = DPE_HEX[bestGrade(dpe.existants, dpe.neufs)] || '#AAAAAA';
          state.corrections.set(id, plainGeom.map(f => ({ ...f, properties: { color } })));
        }
        const neufSeul = dpe.neufs.length > 0 && !dpe.existants.length;
        hatch.setData({ type: 'FeatureCollection', features: neufSeul ? plainGeom : [] });
      }
      render();
      return tileGrade;
    },
    clearSelection() {
      state.selBatId = null; state.addrMarker?.remove(); state.addrMarker = null;
      map.getSource('sel-outline-src')?.setData(EMPTY);
      map.getSource('neuf-hatch-src')?.setData(EMPTY);
    },

    startMeasure(onChange) {
      api.stopMeasure(false);
      state.measure = { points: [], onChange };
      map.getCanvas().classList.add('is-measuring');
      map.doubleClickZoom.disable();
    },
    undoMeasurePoint() {
      if (!state.measure?.points.length) return;
      state.measure.points.pop();
      state.measureMarkers.pop()?.remove();
      drawMeasure();
    },
    stopMeasure(keepShape) {
      map.getCanvas().classList.remove('is-measuring');
      map.doubleClickZoom.enable();
      if (!keepShape) {
        state.measureMarkers.forEach(m => m.remove()); state.measureMarkers = [];
        map.getSource('measure-src')?.setData(EMPTY);
      }
      const done = state.measure; state.measure = null;
      if (keepShape) done?.onChange?.(done.points.slice(), true);
    },
    clearMeasure() {
      state.measureMarkers.forEach(m => m.remove()); state.measureMarkers = [];
      map.getSource('measure-src')?.setData(EMPTY);
      state.measure = null;
      map.getCanvas().classList.remove('is-measuring');
      map.doubleClickZoom.enable();
    },
    isMeasuring: () => !!state.measure,
  };
  return api;
}
