import { useCallback, useEffect, useRef, useState } from 'react';
import maplibregl from 'maplibre-gl';
import area from '@turf/area';
import length from '@turf/length';
import Icon from './components/Icon.jsx';
import Search from './components/Search.jsx';
import LayerIcon from './components/LayerIcon.jsx';
import Fiche from './components/Fiche.jsx';
import { Scale } from './components/Grade.jsx';
import { createMap } from './lib/map.js';
import { fetchDPE, googleStreetView, hasGoogleStreetView, resolveBuilding, reverseAddress } from './lib/data.js';
import { BASEMAPS, HISTORIC, OVERLAYS } from './lib/config.js';

const mobile = !!window.SCOPE_MOBILE;

function formatMeasure(pts) {
  if (pts.length < 2) return null;
  const ring = pts.length >= 3 ? [...pts, pts[0]] : pts;
  const per = Math.round(length({ type: 'Feature', geometry: { type: 'LineString', coordinates: ring }, properties: {} }, { units: 'kilometers' }) * 1000);
  const perStr = per < 1000 ? `${per.toLocaleString('fr-FR')} m` : `${(per / 1000).toLocaleString('fr-FR', { maximumFractionDigits: 2 })} km`;
  if (pts.length < 3) return { surface: null, perimetre: perStr, label: 'Longueur' };
  const a = area({ type: 'Feature', geometry: { type: 'Polygon', coordinates: [ring] }, properties: {} });
  const aStr = a < 10000 ? `${Math.round(a).toLocaleString('fr-FR')} m²` : `${(a / 10000).toLocaleString('fr-FR', { maximumFractionDigits: 2 })} ha`;
  return { surface: aStr, perimetre: perStr, label: 'Périmètre' };
}

export default function App() {
  const mapEl = useRef(null);
  const api = useRef(null);
  const seq = useRef(0);
  const [query, setQuery] = useState('');
  const [place, setPlace] = useState(null);           // { key, label, lngLat, fromClick }
  const [building, setBuilding] = useState({ status: 'idle' });
  const [dpe, setDpe] = useState({ status: 'idle' });
  const [tileGrade, setTileGrade] = useState(null);
  const [overlays, setOverlays] = useState({});
  const [basemap, setBasemap] = useState('ortho');
  const [historic, setHistoric] = useState(null);     // null = fermé, '' = ouvert sans année
  const [measure, setMeasure] = useState(null);       // { points, done }
  const [panel, setPanel] = useState(null);           // 'layers' | 'basemap' | 'tools' | 'sources'
  const [street, setStreet] = useState(null);
  const [toast, setToast] = useState(null);
  const [locating, setLocating] = useState(false);
  const [sheet, setSheet] = useState('half');
  const toastTimer = useRef(null);
  const meMarker = useRef(null);
  const watchId = useRef(null);

  const say = useCallback((msg, ms) => {
    clearTimeout(toastTimer.current);
    setToast(msg);
    if (msg && ms) toastTimer.current = setTimeout(() => setToast(null), ms);
  }, []);

  // ── Sélection d'un bien (adresse saisie ou clic) ──
  const selectPlace = useCallback(async (feature, opts = {}) => {
    const id = ++seq.current;
    const [lng, lat] = opts.clickAt || feature.geometry.coordinates;
    const isClick = !!opts.clickAt;
    setQuery(feature.properties.label || '');
    setPlace({ key: `${id}`, label: feature.properties.label || '', lngLat: [lng, lat], fromClick: isClick });
    setBuilding({ status: 'loading' }); setDpe({ status: 'loading' }); setTileGrade(opts.tileGrade || null);
    setPanel(null); setSheet('half');
    const m = api.current;
    m.flyTo([lng, lat]); m.placeMarker([lng, lat]); m.select(null);

    const data = await resolveBuilding({ lat, lng, feature, bldGroupId: opts.bldGroupId, isClick });
    if (id !== seq.current) return;
    setBuilding({ status: 'ok', data });
    if (isClick && data.bdnb?.libelle_adr_principale_ban) setQuery(data.bdnb.libelle_adr_principale_ban);
    m.select(data.batId);
    try {
      const d = await fetchDPE(data.banIds);
      if (id !== seq.current) return;
      setDpe({ status: 'ok', ...d });
      const g = m.applySelection(d);
      if (g) setTileGrade(g);
      m.map.once('idle', () => { if (id === seq.current) { const g2 = m.applySelection(d); if (g2) setTileGrade(g2); } });
    } catch {
      if (id === seq.current) setDpe({ status: 'error' });
    }
  }, []);

  // ── Carte ──
  useEffect(() => {
    api.current = createMap(mapEl.current, {
      onStatus: say,
      mobile,
      onPick: async ({ geocodeAt, clickAt, bldGroupId, tileGrade: tg }) => {
        say('Recherche de l’adresse…');
        try {
          const f = await reverseAddress(geocodeAt[0], geocodeAt[1]);
          say(null);
          if (f) selectPlace(f, { clickAt, bldGroupId, tileGrade: tg });
          else say('Aucune adresse trouvée ici', 2500);
        } catch { say('Service d’adresses indisponible', 2500); }
      },
    });
    window._map = api.current.map; // pratique pour les tests
    return () => api.current.map.remove();
  }, [say, selectPlace]);

  function closeFiche() {
    seq.current++;
    setPlace(null); setBuilding({ status: 'idle' }); setDpe({ status: 'idle' }); setTileGrade(null);
    api.current.clearSelection();
  }
  async function toggleOverlay(id, on = !overlays[id]) {
    setOverlays(o => ({ ...o, [id]: on }));
    const ok = await api.current.setOverlay(id, on);
    if (!ok) setOverlays(o => ({ ...o, [id]: false }));
  }
  async function chooseBasemap(k) {
    const prev = basemap;
    setBasemap(k); setHistoric(null);
    if (!(await api.current.setBasemap(k))) setBasemap(prev);
  }
  function chooseHistoric(id) {
    const next = historic === id ? '' : id;
    setHistoric(next); api.current.setHistoric(next || null);
  }
  function toggleHistoric() {
    if (historic !== null) { setHistoric(null); api.current.setHistoric(null); }
    else { setHistoric(''); setPanel(null); }
  }
  function toggleMeasure() {
    const m = api.current;
    if (measure && !measure.done) { m.stopMeasure(false); setMeasure(null); return; }
    if (measure?.done) { m.clearMeasure(); setMeasure(null); return; }
    setPanel(null);
    if (mobile) setSheet('peek');
    setMeasure({ points: [], done: false });
    m.startMeasure((points, done) => setMeasure({ points, done: !!done }));
  }
  function finishMeasure() {
    if ((measure?.points.length || 0) >= 2) api.current.stopMeasure(true);
    else { api.current.stopMeasure(false); setMeasure(null); }
  }
  // Photo de rue Google du bien sélectionné (cherchée une fois par bien, partagée avec l'aperçu de la fiche)
  const [sv, setSv] = useState({ status: 'idle' });
  useEffect(() => {
    if (!place || !hasGoogleStreetView()) { setSv({ status: 'idle' }); return; }
    let alive = true;
    setSv({ status: 'loading', key: place.key });
    googleStreetView(place.lngLat[0], place.lngLat[1])
      .then(r => alive && setSv({ status: r.found ? 'ok' : 'none', key: place.key, ...r }))
      .catch(() => alive && setSv({ status: 'error', key: place.key }));
    return () => { alive = false; };
  }, [place]);

  // Vue de rue interactive
  async function openStreet() {
    if (!hasGoogleStreetView()) { say('Vue de rue non configurée', 3000); return; }
    if (place && sv.key === place.key && sv.status === 'ok') { setStreet({ url: sv.url }); return; }
    const at = place?.lngLat || api.current.map.getCenter().toArray();
    say('Recherche de la vue de rue…');
    try {
      const g = await googleStreetView(at[0], at[1]);
      if (g.found) { say(null); setStreet({ url: g.url }); }
      else say('Aucune photo de rue Google à proximité', 3000);
    } catch { say('Vue de rue indisponible pour le moment', 3000); }
  }
  function locate() {
    const m = api.current.map;
    if (watchId.current != null) {
      navigator.geolocation.clearWatch(watchId.current); watchId.current = null;
      meMarker.current?.remove(); meMarker.current = null; setLocating(false); return;
    }
    if (!navigator.geolocation) { say('Géolocalisation indisponible sur cet appareil', 3000); return; }
    setLocating(true);
    let first = true;
    watchId.current = navigator.geolocation.watchPosition(p => {
      const ll = [p.coords.longitude, p.coords.latitude];
      if (!meMarker.current) { const el = document.createElement('div'); el.className = 'me-dot'; meMarker.current = new maplibregl.Marker({ element: el }).setLngLat(ll).addTo(m); }
      else meMarker.current.setLngLat(ll);
      if (first) { first = false; m.flyTo({ center: ll, zoom: 17, speed: 1.6 }); }
    }, () => { say('Impossible d’obtenir votre position', 3000); setLocating(false); watchId.current = null; }, { enableHighAccuracy: true, timeout: 10000 });
  }

  const nbOverlays = Object.values(overlays).filter(Boolean).length;
  const mres = measure ? formatMeasure(measure.points) : null;
  const togglePanel = p => setPanel(cur => (cur === p ? null : p));

  // Fiche commune PC / mobile
  const fiche = place && (
    <Fiche place={place} building={building} dpe={dpe} tileGrade={tileGrade} mobile={mobile} streetPreview={sv.key === place?.key ? sv : null}
      onClose={closeFiche} onStreetview={openStreet}
      onShowLayer={id => { toggleOverlay(id, true); if (mobile) setSheet('peek'); say('Couche affichée sur la carte', 2000); }} />
  );

  return (
    <div className={`app${mobile ? ' mobile' : ''}`} style={mobile ? { '--top-h': '120px' } : undefined}>
      <div ref={mapEl} className="map" aria-label="Carte" role="region" />

      <div className="topbar">
        <Search value={query} onValue={setQuery} onSelect={f => selectPlace(f)} getCenter={() => api.current?.map.getCenter()} mobile={mobile} />
        {!mobile && (
          <>
            <div className="spacer" />
            <div className="seg glass" role="group" aria-label="Fond de carte">
              {Object.entries(BASEMAPS).map(([k, b]) => (
                <button key={k} aria-pressed={basemap === k && !historic} onClick={() => chooseBasemap(k)}>{b.label}</button>
              ))}
            </div>
            <button className="pill-btn glass" aria-expanded={panel === 'layers'} onClick={() => togglePanel('layers')}>
              <Icon name="layers" size={18} /> Couches {nbOverlays > 0 && <span className="count">{nbOverlays}</span>}
            </button>
            <button className="pill-btn glass" aria-label="Sources des données" onClick={() => togglePanel('sources')}><Icon name="sources" size={18} /></button>
          </>
        )}
      </div>

      {/* Couches */}
      {panel === 'layers' && (
        <div className="dropdown glass" role="dialog" aria-label="Couches">
          <div className="row" style={{ justifyContent: 'space-between', marginBottom: 8 }}>
            <h2 style={{ margin: 0 }}>Couches</h2>
            <button className="icon-btn" aria-label="Fermer les couches" onClick={() => setPanel(null)}><Icon name="x" size={18} /></button>
          </div>
          {[...new Set(OVERLAYS.map(o => o.group))].map(g => (
            <div key={g}>
              <h3>{g}</h3>
              <div className="layer-grid">
                {OVERLAYS.filter(o => o.group === g).map(o => (
                  <button key={o.id} className="layer-tile" aria-pressed={!!overlays[o.id]} onClick={() => { toggleOverlay(o.id); setPanel(null); }}>
                    <LayerIcon id={o.id} />
                    <span>{o.label}</span>
                    {overlays[o.id] && <span className="layer-check" aria-hidden="true"><Icon name="check" size={12} stroke={3} /></span>}
                  </button>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
      {/* Fonds de carte (mobile) */}
      {panel === 'basemap' && (
        <div className="dropdown glass" role="dialog" aria-label="Fond de carte">
          <h2>Fond de carte</h2>
          <div className="list">
            {Object.entries(BASEMAPS).map(([k, b]) => (
              <button key={k} className={`btn ${basemap === k && !historic ? 'primary' : 'ghost'}`} aria-pressed={basemap === k && !historic} onClick={() => { chooseBasemap(k); setPanel(null); }}>{b.label}</button>
            ))}
            <button className={`btn ${historic !== null ? 'primary' : 'ghost'}`} onClick={() => { toggleHistoric(); setPanel(null); }}><Icon name="clock" /> Photos anciennes</button>
          </div>
        </div>
      )}
      {/* Outils (mobile) */}
      {panel === 'tools' && (
        <div className="dropdown glass" role="dialog" aria-label="Outils">
          <h2>Outils</h2>
          <div className="list">
            <button className="btn ghost" style={{ justifyContent: 'flex-start' }} onClick={toggleMeasure}><Icon name="ruler" /> Mesurer une surface</button>
            <button className="btn ghost" style={{ justifyContent: 'flex-start' }} onClick={() => { setPanel(null); openStreet(); }}><Icon name="camera" /> Vue de rue</button>
            <button className="btn ghost" style={{ justifyContent: 'flex-start' }} onClick={() => { setPanel(null); locate(); }}><Icon name="locate" /> {locating ? 'Arrêter la localisation' : 'Me localiser'}</button>
            <button className="btn ghost" style={{ justifyContent: 'flex-start' }} onClick={() => setPanel('sources')}><Icon name="sources" /> Sources des données</button>
          </div>
        </div>
      )}

      {/* Fiche ou accueil */}
      {!mobile && place && (
        <section className="fiche glass" aria-label="Fiche du bien"><div className="fiche-scroll">{fiche}</div></section>
      )}
      {mobile && place && <Sheet state={sheet} setState={setSheet}>{fiche}</Sheet>}

      {/* Mesure */}
      {measure && (
        <div className="floating measure-panel glass" role="status" aria-live="polite">
          <div className="row" style={{ justifyContent: 'space-between' }}>
            <span className="fact-label">{measure.done ? 'Mesure' : `Mesure en cours · ${measure.points.length} point${measure.points.length > 1 ? 's' : ''}`}</span>
            <button className="icon-btn" aria-label="Fermer la mesure" onClick={() => { api.current.clearMeasure(); setMeasure(null); }}><Icon name="x" size={18} /></button>
          </div>
          <div className="measure-values">
            <div className="glass-in"><span className="small muted">Surface au sol</span><strong>{mres?.surface || '—'}</strong></div>
            <div className="glass-in"><span className="small muted">{mres?.label || 'Périmètre'}</span><strong>{mres?.perimetre || '—'}</strong></div>
          </div>
          {!measure.done && (
            <>
              <div className="hint">{mobile ? 'Touchez' : 'Cliquez sur'} la carte pour ajouter des points{mobile ? '.' : ', double-cliquez pour terminer.'}</div>
              <div className="btn-row">
                <button className="btn ghost" onClick={() => api.current.undoMeasurePoint()} disabled={!measure.points.length}><Icon name="undo" size={18} /> Annuler le point</button>
                <button className="btn primary" onClick={finishMeasure}><Icon name="check" size={18} stroke={2.2} /> Terminer</button>
              </div>
            </>
          )}
          {measure.done && <button className="btn ghost" onClick={toggleMeasure}>Effacer la mesure</button>}
        </div>
      )}

      {/* Photos aériennes anciennes */}
      {historic !== null && (
        <div className="floating historic glass" role="group" aria-label="Photos aériennes anciennes (IGN)">
          <span className="small" style={{ fontWeight: 700, padding: '0 6px' }}>Photos IGN</span>
          {HISTORIC.map(h => <button key={h.id} aria-pressed={historic === h.id} onClick={() => chooseHistoric(h.id)}>{h.label}</button>)}
          <button className="icon-btn" aria-label="Fermer les photos anciennes" onClick={toggleHistoric}><Icon name="x" size={16} /></button>
        </div>
      )}

      {toast && <div className="floating toast" role="status">{toast}</div>}

      {/* Outils et contrôles (PC) */}
      {!mobile && (
        <>
          <div className="toolbar glass" role="toolbar" aria-label="Outils">
            <button aria-pressed={!!measure} onClick={toggleMeasure}><Icon name="ruler" />Mesurer</button>
            <button aria-pressed={historic !== null} onClick={toggleHistoric}><Icon name="clock" />Photos anciennes</button>
            <button aria-pressed={!!street} onClick={() => (street ? setStreet(null) : openStreet())}><Icon name="camera" />Vue de rue</button>
          </div>
          <div className="mapctrl">
            <div className="glass"><button aria-label={locating ? 'Arrêter la localisation' : 'Me localiser'} aria-pressed={locating} onClick={locate}><Icon name="locate" /></button></div>
            <div className="glass">
              <button aria-label="Zoomer" onClick={() => api.current.map.zoomIn()}><Icon name="plus" /></button>
              <button aria-label="Dézoomer" onClick={() => api.current.map.zoomOut()}><Icon name="minus" /></button>
            </div>
          </div>
          {overlays.dpe && <div className="legend glass">Étiquette DPE des bâtiments<Scale small /></div>}
        </>
      )}

      {/* Vue de rue */}
      {street && (
        <div className="streetview glass" role="dialog" aria-label="Vue de rue">
          <header>Vue de rue · Google Street View<span className="spacer" />
            <button className="icon-btn" aria-label="Fermer la vue de rue" onClick={() => setStreet(null)}><Icon name="x" size={18} /></button>
          </header>
          <iframe key={street.url} title="Vue de rue" src={street.url} allow="fullscreen; accelerometer; gyroscope" allowFullScreen loading="lazy" referrerPolicy="no-referrer-when-downgrade" />
        </div>
      )}

      {panel === 'sources' && <Sources onClose={() => setPanel(null)} meta={api.current?.meta()} />}

      {/* Navigation du bas (mobile) */}
      {mobile && (
        <nav className="bottomnav glass" aria-label="Navigation">
          <button aria-pressed={!panel && !!place} onClick={() => { setPanel(null); if (place) setSheet(s => (s === 'peek' ? 'half' : s)); }}><Icon name="building" size={22} />Fiche</button>
          <button aria-pressed={panel === 'layers'} onClick={() => togglePanel('layers')}><Icon name="layers" size={22} />Couches{nbOverlays ? ` (${nbOverlays})` : ''}</button>
          <button aria-pressed={panel === 'basemap' || historic !== null} onClick={() => togglePanel('basemap')}><Icon name="map" size={22} />Fonds</button>
          <button aria-pressed={panel === 'tools' || !!measure} onClick={() => togglePanel('tools')}><Icon name="ruler" size={22} />Outils</button>
        </nav>
      )}
    </div>
  );
}

// Panneau glissant (mobile) : trois hauteurs, on le tire par la poignée ou par le contenu quand il est en haut
function Sheet({ state, setState, children }) {
  const ref = useRef(null), scroll = useRef(null), drag = useRef(null);
  const [h, setH] = useState(null);
  const heights = () => {
    const H = window.innerHeight - 76;
    return { peek: 128, half: Math.round(H * 0.52), full: H - 72 };
  };
  const target = heights()[state];
  function start(e, fromHandle) {
    if (!fromHandle && scroll.current.scrollTop > 0) return;
    drag.current = { y0: e.touches[0].clientY, h0: ref.current.offsetHeight, moved: false, fromHandle };
  }
  function move(e) {
    const d = drag.current; if (!d) return;
    const dy = e.touches[0].clientY - d.y0;
    if (!d.moved) {
      if (Math.abs(dy) < 8) return;
      if (!d.fromHandle && dy < 0 && state === 'full') { drag.current = null; return; } // laisser défiler
      d.moved = true;
    }
    const hs = heights();
    setH(Math.max(hs.peek, Math.min(hs.full, d.h0 - dy)));
  }
  function end() {
    const d = drag.current; drag.current = null;
    if (!d?.moved || h == null) { setH(null); return; }
    const hs = heights();
    const best = Object.entries(hs).sort((a, b) => Math.abs(a[1] - h) - Math.abs(b[1] - h))[0][0];
    setState(best); setH(null);
  }
  const cycle = () => setState(state === 'peek' ? 'half' : state === 'half' ? 'full' : 'peek');
  return (
    <section ref={ref} className={`sheet glass${h != null ? ' dragging' : ''}`} style={{ height: h ?? target }} aria-label="Fiche du bien"
      onTouchMove={move} onTouchEnd={end} onTouchCancel={end}>
      <button className="sheet-handle" onTouchStart={e => start(e, true)} onClick={cycle}
        aria-label={state === 'full' ? 'Réduire la fiche' : 'Agrandir la fiche'}><span /></button>
      <div ref={scroll} className="fiche-scroll" onTouchStart={e => start(e, false)}
        style={{ overflowY: state === 'peek' ? 'hidden' : 'auto' }}
        onClick={() => { if (state === 'peek') setState('half'); }}>
        {children}
      </div>
    </section>
  );
}

function Sources({ onClose, meta }) {
  useEffect(() => {
    const k = e => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', k);
    return () => window.removeEventListener('keydown', k);
  }, [onClose]);
  const rows = [
    ['ADEME', 'DPE des logements (existants et neufs)', 'Temps réel'],
    ['BDNB (CSTB)', 'Bâtiments, rattachement des adresses', 'Temps réel'],
    ['BDNB (CSTB)', 'Couleurs DPE de la carte', meta?.millesime ? `Édition ${meta.millesime}` : 'Semestrielle'],
    ['Base adresse nationale', 'Recherche d’adresse', 'Temps réel'],
    ['IGN Géoplateforme', 'Fonds de carte, photos aériennes, cadastre', 'Temps réel'],
    ['Géoportail de l’urbanisme', 'PLU, monuments historiques, sites', 'Temps réel'],
    ['France Chaleur Urbaine', 'Réseaux de chaleur', 'Mensuelle'],
    ['GRDF', 'Réseau de gaz', 'Mensuelle'],
    ['RNIC', 'Registre des copropriétés', 'Temps réel'],
    ['DVF (DGFiP)', 'Ventes immobilières', 'Semestrielle'],
    ['Google Street View', 'Photos de rue', 'Temps réel'],
  ];
  return (
    <div className="modal-back" onClick={onClose}>
      <div className="modal" role="dialog" aria-modal="true" aria-labelledby="src-title" onClick={e => e.stopPropagation()}>
        <div className="row" style={{ justifyContent: 'space-between' }}>
          <h2 id="src-title">Sources des données</h2>
          <button className="icon-btn" aria-label="Fermer" onClick={onClose} autoFocus><Icon name="x" /></button>
        </div>
        <p className="muted small" style={{ margin: 0 }}>Données publiques ouvertes. Scope n’affiche un DPE que s’il est rattaché de façon certaine au bâtiment.</p>
        <table className="src-table">
          <thead><tr><th>Source</th><th>Données</th><th>Mise à jour</th></tr></thead>
          <tbody>{rows.map((r, i) => <tr key={i}><td><strong>{r[0]}</strong></td><td>{r[1]}</td><td>{r[2]}</td></tr>)}</tbody>
        </table>
        <p className="small muted">Ancienne version : <a href="/classic.html">Scope classique</a></p>
      </div>
    </div>
  );
}
