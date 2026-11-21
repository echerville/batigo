import { useEffect, useState } from 'react';
import Icon from './Icon.jsx';
import { Grade, Scale } from './Grade.jsx';
import { ADEME_DPE_URL } from '../lib/config.js';
import { fetchAltitude, fetchDVF, fetchPLUZone, fetchRNC } from '../lib/data.js';
import {
  IMMEUBLE, bdnbEnergie, bestGrade, frDate, installType, preCoeffGrade, referenceDPE, shortEnergie,
  splitAddress, summary, usageLabel, zoneClimatique,
} from '../lib/dpe.js';

const TABS = [['batiment', 'Bâtiment'], ['dpe', 'DPE'], ['urbanisme', 'Urbanisme'], ['parcelle', 'Parcelle']];
const today = new Date().toLocaleDateString('fr-FR');

function Fact({ label, value, note }) {
  return (
    <div className="fact glass-in">
      <div className="fact-label">{label}</div>
      <div className="fact-value">{value ?? <span className="na">-</span>}</div>
      {note && <div className="fact-note">{note}</div>}
    </div>
  );
}
const Loading = ({ text }) => <div className="empty glass-in"><span className="spin" />{text}</div>;
const energy = (e, t) => (e ? (t ? `${e} · ${t}` : e) : null);

// Charge une donnée quand l'onglet s'ouvre (une fois par bien)
function useLazy(enabled, key, loader) {
  const [s, set] = useState({ status: 'idle', key: null });
  useEffect(() => {
    if (!enabled) return;
    let alive = true;
    set({ status: 'loading', key });
    loader().then(data => alive && set({ status: 'ok', data, key })).catch(() => alive && set({ status: 'error', key }));
    return () => { alive = false; };
  }, [enabled, key]); // eslint-disable-line react-hooks/exhaustive-deps
  return s.key === key ? s : { status: enabled ? 'loading' : 'idle' };
}

export default function Fiche({ place, building, dpe, tileGrade, onStreetview, onClose, onShowLayer, mobile, streetPreview }) {
  const [tab, setTab] = useState('batiment');
  useEffect(() => { setTab('batiment'); }, [place.key]);

  const b = building.data;
  const bdnb = b?.bdnb;
  const label = (place.fromClick && bdnb?.libelle_adr_principale_ban) || place.label;
  const [l1, l2] = splitAddress(label);
  const dpeOk = dpe.status === 'ok';
  const ex = dpe.existants || [], ne = dpe.neufs || [];
  const sum = dpeOk ? summary(ex, ne) : null;
  const grade = dpeOk ? bestGrade(ex, ne) : null;
  const ref = dpeOk ? referenceDPE(ex, ne) : null;
  const nbDpe = ex.length + ne.length;
  const [alt, setAlt] = useState({ key: null, z: null });
  useEffect(() => {
    let off = false;
    const [lng, lat] = place.lngLat || [];
    if (lng == null) return;
    fetchAltitude(lng, lat).then(z => { if (!off) setAlt({ key: place.key, z }); }).catch(() => { if (!off) setAlt({ key: place.key, z: null }); });
    return () => { off = true; };
  }, [place.key]);
  const zone = zoneClimatique(place.citycode || bdnb?.code_commune_insee);
  const site = { zone, alt: alt.key === place.key ? alt.z : undefined };

  return (
    <>
      <div className="row">
        <span className="kicker">{usageLabel(bdnb?.usage_principal_bdnb_open) || 'Adresse'}</span>
        {building.status === 'loading' && <span className="tag muted"><span className="spin" style={{ width: 12, height: 12, margin: 0 }} /> Recherche du bâtiment</span>}
        {building.status === 'ok' && bdnb && <span className="tag ok"><Icon name="check" size={12} stroke={2.4} /> Bâtiment identifié</span>}
        {building.status === 'ok' && !bdnb && <span className="tag warn">Bâtiment non rattaché</span>}
      </div>
      <div>
        <h1 className="addr">{l1 || label}</h1>
        {l2 && <div className="addr-sub">{l2}</div>}
      </div>
      {onClose && !mobile && <button className="icon-btn fiche-close" onClick={onClose} aria-label="Fermer la fiche"><Icon name="x" /></button>}

      <StreetPreview sv={streetPreview} onOpen={onStreetview} />

      <div className="tabs" role="tablist" aria-label="Rubriques de la fiche">
        {TABS.map(([id, name]) => (
          <button key={id} role="tab" id={`tab-${id}`} aria-selected={tab === id} aria-controls={`panel-${id}`} onClick={() => setTab(id)}>
            {name}{id === 'dpe' && dpeOk && nbDpe > 0 ? ` (${nbDpe})` : ''}
          </button>
        ))}
      </div>

      <div role="tabpanel" id={`panel-${tab}`} aria-labelledby={`tab-${tab}`} style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
        {tab === 'batiment' && (
          <BatimentTab {...{ building, bdnb, dpe, dpeOk, sum, grade, ref, tileGrade, onStreetview, setTab, nbDpe, site }} />
        )}
        {tab === 'dpe' && <DpeTab dpe={dpe} />}
        {tab === 'urbanisme' && <UrbaTab place={place} onShowLayer={onShowLayer} />}
        {tab === 'parcelle' && <ParcelleTab place={place} building={building} />}
      </div>

      <div className="small muted">Sources : ADEME, BDNB (CSTB), IGN, DGFiP · consulté le {today}</div>
    </>
  );
}

// Aperçu Google Street View du bâtiment ; un clic ouvre la vue de rue interactive
function StreetPreview({ sv, onOpen }) {
  const [failed, setFailed] = useState(false);
  useEffect(() => { setFailed(false); }, [sv?.preview]);
  if (!sv || sv.status === 'idle' || sv.status === 'error') return null;
  if (sv.status === 'loading') return <div className="sv-thumb skeleton" aria-hidden="true" />;
  if (sv.status === 'none' || failed) return <div className="sv-none small muted">Pas de photo de rue Google à proximité.</div>;
  return (
    <button className="sv-thumb" onClick={onOpen} aria-label="Ouvrir la vue de rue">
      <img src={sv.preview} alt="Vue de rue du bâtiment (Google Street View)" onError={() => setFailed(true)} />
      <span className="sv-label"><Icon name="camera" size={14} /> Vue de rue</span>
    </button>
  );
}

function SiteFacts({ site }) {
  return (
    <div className="facts">
      <Fact label="Zone climatique" value={site.zone} />
      <Fact label="Altitude" value={site.alt === undefined ? '…' : site.alt != null ? `${Math.round(site.alt)} m` : null}
        />
    </div>
  );
}

function BatimentTab({ building, bdnb, dpe, dpeOk, sum, grade, ref, tileGrade, onStreetview, setTab, nbDpe, site }) {
  if (building.status === 'ok' && !bdnb) {
    return (
      <>
        <div className="empty glass-in">
          <strong>Aucun bâtiment n’est rattaché de façon certaine à cette adresse.</strong>
          Scope n’affiche pas de bâtiment voisin. Cliquez directement sur le bâtiment dans la carte pour l’identifier.
        </div>
        <SiteFacts site={site} />
      </>
    );
  }
  return (
    <>
      <div className="card glass-in">
        <div className="card-title">Étiquette énergie {ref && <span>DPE du {frDate(ref.date_reception_dpe)}</span>}</div>
        {dpe.status === 'loading' && <div className="skeleton" style={{ height: 40 }} />}
        {dpeOk && grade && <Scale active={grade} />}
        {dpeOk && grade && (
          <div className="row small muted">
            {ref?.conso_5_usages_par_m2_ep && <span><strong style={{ color: 'var(--ink)' }}>{Math.round(ref.conso_5_usages_par_m2_ep)}</strong> kWh/m²/an</span>}
            {ref?.etiquette_ges && <span>GES <strong style={{ color: 'var(--ink)' }}>{ref.etiquette_ges}</strong></span>}
            {nbDpe > 1 && <span>{ref?.methode_application_dpe === IMMEUBLE ? 'DPE de l’immeuble' : `synthèse de ${nbDpe} DPE`}</span>}
          </div>
        )}
        {dpeOk && !grade && (
          <div className="small muted">Aucun DPE publié par l’ADEME pour ce bâtiment.
            {tileGrade && <> La couleur sur la carte (étiquette {tileGrade}) vient de la BDNB et peut correspondre à un DPE plus ancien.</>}</div>
        )}
        {dpeOk && grade && tileGrade && tileGrade !== grade && (
          <div className="small muted">Carte : étiquette {tileGrade} (BDNB). DPE ADEME vérifié : {grade}.</div>
        )}
        {dpe.status === 'error' && <div className="small muted">DPE indisponibles pour le moment (ADEME).</div>}
      </div>

      {building.status === 'loading' ? (
        <div className="facts">{[0, 1, 2, 3].map(i => <div key={i} className="fact glass-in"><div className="skeleton" /><div className="skeleton" style={{ marginTop: 8, width: '60%' }} /></div>)}</div>
      ) : (
        <div className="facts">
          <Fact label="Construction" value={bdnb?.annee_construction || sum?.year} />
          <Fact label="Emprise au sol" value={bdnb?.surface_emprise_sol ? `${Math.round(bdnb.surface_emprise_sol)} m²` : null} />
          <Fact label="Chauffage"
            value={sum?.chauffage ? energy(sum.chauffage, sum.chauffageType) : bdnbEnergie(bdnb?.type_energie_chauffage)}
            />
          <Fact label="Eau chaude"
            value={sum?.ecs ? energy(sum.ecs, sum.ecsType) : bdnbEnergie(bdnb?.type_generateur_ecs)}
            />
          {sum?.surface && <Fact label="Surface (DPE)" value={sum.surface} />}
          {bdnb?.nb_log > 0 && <Fact label="Logements" value={bdnb.nb_log} />}
          <Fact label="Zone climatique" value={site.zone} />
          <Fact label="Altitude" value={site.alt === undefined ? '…' : site.alt != null ? `${Math.round(site.alt)} m` : null}
            />
        </div>
      )}

    </>
  );
}

function DpeRow({ r, neuf }) {
  const old = preCoeffGrade(r);
  const surf = r.surface_habitable_logement ? `${Math.round(r.surface_habitable_logement)} m²` : null;
  const ch = shortEnergie(r.type_energie_principale_chauffage);
  const it = installType(r.type_installation_chauffage_n1);
  const what = [r.complement_adresse_logement, surf].filter(Boolean).join(' · ') || 'Logement';
  return (
    <div className="list-row glass-in">
      <Grade g={r.etiquette_dpe} from={old} hatch={neuf} />
      <div className="main">
        <div className="t">{what}</div>
        <div className="s">{[ch && (it ? `${ch} ${it}` : ch), frDate(r.date_reception_dpe), r.annee_construction || r.periode_construction].filter(Boolean).join(' · ')}</div>
      </div>
      {r.numero_dpe && <a className="ext" href={ADEME_DPE_URL(r.numero_dpe)} target="_blank" rel="noopener noreferrer" aria-label={`Voir le DPE ${r.numero_dpe} sur le site de l’ADEME`}><Icon name="ext" size={18} /></a>}
    </div>
  );
}

function DpeTab({ dpe }) {
  const [all, setAll] = useState(false);
  if (dpe.status === 'loading' || dpe.status === 'idle') return <Loading text="Recherche des DPE auprès de l’ADEME…" />;
  if (dpe.status === 'error') return <div className="empty glass-in">DPE indisponibles pour le moment (ADEME). Réessayez dans un instant.</div>;
  const imm = dpe.existants.filter(r => r.methode_application_dpe === IMMEUBLE);
  const logs = dpe.existants.filter(r => r.methode_application_dpe !== IMMEUBLE);
  const neufs = dpe.neufs;
  if (!imm.length && !logs.length && !neufs.length) {
    return (
      <div className="empty glass-in">
        <strong>Aucun DPE publié par l’ADEME pour ce bâtiment.</strong>
        Seuls les DPE rattachés de façon certaine aux adresses du bâtiment sont affichés. Un DPE très récent peut ne pas être encore publié.
      </div>
    );
  }
  const shown = all ? logs : logs.slice(0, 10);
  const counts = {};
  logs.forEach(r => { counts[r.etiquette_dpe] = (counts[r.etiquette_dpe] || 0) + 1; });
  return (
    <>
      {imm[0] && (
        <div className="card glass-in">
          <div className="card-title">DPE de l’immeuble <span>{frDate(imm[0].date_reception_dpe)}</span></div>
          <div className="row" style={{ gap: 12 }}>
            <Grade g={imm[0].etiquette_dpe} size="lg" />
            <div className="small">
              {[imm[0].nombre_appartement && `${imm[0].nombre_appartement} logements`,
                imm[0].surface_habitable_immeuble && `${Math.round(imm[0].surface_habitable_immeuble)} m²`,
                shortEnergie(imm[0].type_energie_principale_chauffage)].filter(Boolean).join(' · ')}
            </div>
            {imm[0].numero_dpe && <a className="ext" style={{ marginLeft: 'auto' }} href={ADEME_DPE_URL(imm[0].numero_dpe)} target="_blank" rel="noopener noreferrer" aria-label="Voir le DPE de l’immeuble sur le site de l’ADEME"><Icon name="ext" size={18} /></a>}
          </div>
          {imm.length > 1 && <div className="small muted">{imm.length} DPE d’immeuble publiés, le plus récent est affiché.</div>}
        </div>
      )}
      {logs.length > 0 && (
        <>
          <div className="section-title">Logements · {logs.length} DPE</div>
          <div className="row">{Object.keys(counts).sort().map(g => <span key={g} className="row" style={{ gap: 4 }}><Grade g={g} /><span className="small">{counts[g]}</span></span>)}</div>
          <div className="list">{shown.map(r => <DpeRow key={r.numero_dpe} r={r} />)}</div>
          {logs.length > 10 && !all && <button className="btn ghost" onClick={() => setAll(true)}>Afficher les {logs.length - 10} autres</button>}
        </>
      )}
      {neufs.length > 0 && (
        <>
          <div className="section-title">Logements neufs · {neufs.length} DPE</div>
          <div className="list">{neufs.slice(0, 20).map(r => <DpeRow key={r.numero_dpe} r={r} neuf />)}</div>
        </>
      )}
      <div className="small muted">« F→E » : étiquette avant le changement du coefficient de l’électricité au 1er janvier 2026.</div>
    </>
  );
}

function UrbaTab({ place, onShowLayer }) {
  const plu = useLazy(true, place.key, () => fetchPLUZone(place.lngLat[1], place.lngLat[0]));
  return (
    <>
      <div className="card glass-in">
        <div className="card-title">Zone du PLU</div>
        {plu.status === 'loading' && <div className="skeleton" />}
        {plu.status === 'error' && <div className="small muted">Géoportail de l’urbanisme indisponible pour le moment.</div>}
        {plu.status === 'ok' && (plu.data ? (
          <>
            <div className="row"><span className="tag info" style={{ fontSize: 14 }}>{plu.data.libelle || '?'}</span>{plu.data.libelong && <span className="small">{plu.data.libelong}</span>}</div>
            {plu.data.partition && <div className="small muted">Document {plu.data.partition}{plu.data.nom_commune ? ` · ${plu.data.nom_commune}` : ''}</div>}
          </>
        ) : <div className="small muted">Aucune zone de PLU publiée à cet endroit (commune au RNU ou document non numérisé).</div>)}
      </div>
      <div className="card glass-in">
        <div className="card-title">Protections patrimoniales</div>
        <div className="small muted">Affichez-les sur la carte pour vérifier si le bien est concerné.</div>
        <div className="list">
          {[['ac1', 'Abords de monuments historiques'], ['ac4', 'Sites patrimoniaux remarquables'], ['ac2', 'Sites classés ou inscrits'], ['plu', 'Zones du PLU']].map(([id, l]) => (
            <button key={id} className="btn ghost" style={{ justifyContent: 'flex-start' }} onClick={() => onShowLayer(id)}><Icon name="layers" size={18} /> {l}</button>
          ))}
        </div>
      </div>
    </>
  );
}

function ParcelleTab({ place, building }) {
  const b = building.data;
  const cad = b?.cadastre;
  const rnc = useLazy(!!b?.rnc, place.key, () => fetchRNC(b.rnc));
  const dvf = useLazy(!!b?.idu, place.key, () => fetchDVF(b.idu));
  if (building.status === 'loading') return <Loading text="Recherche de la parcelle…" />;
  const eur = v => (v ? new Intl.NumberFormat('fr-FR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }).format(v) : '—');
  return (
    <>
      <div className="facts">
        <Fact label="Section" value={cad?.section} />
        <Fact label="Numéro" value={cad?.confident ? cad?.numero : null} note={cad && !cad.confident ? 'Parcelle incertaine' : null} />
        {cad?.contenance && <Fact label="Contenance" value={`${Number(cad.contenance).toLocaleString('fr-FR')} m²`} />}
        {cad?.idu && <Fact label="Référence" value={<span style={{ fontSize: 14 }}>{cad.idu}</span>} />}
      </div>

      <div className="card glass-in">
        <div className="card-title">Copropriété <span>Registre national (RNIC)</span></div>
        {!b?.rnc && <div className="small muted">Adresse non identifiée.</div>}
        {rnc.status === 'loading' && <div className="skeleton" />}
        {rnc.status === 'error' && <div className="small muted">Registre indisponible pour le moment.</div>}
        {rnc.status === 'ok' && (rnc.data ? (
          <div className="facts">
            <Fact label="Nom" value={rnc.data.nom_usage_copropriete} />
            <Fact label="Syndic" value={rnc.data.raison_sociale_representant_legal} />
            <Fact label="Lots" value={rnc.data.nombre_total_lots} note={rnc.data.nombre_lots_habitation ? `${rnc.data.nombre_lots_habitation} d’habitation` : null} />
            <Fact label="Construction" value={rnc.data.periode_construction} />
          </div>
        ) : <div className="small muted">Aucune copropriété répertoriée à cette adresse.</div>)}
        {rnc.status === 'ok' && rnc.data && !b?.rnc?.idu && <div className="small muted">Trouvée par proximité de l’adresse : à vérifier.</div>}
      </div>

      <div className="card glass-in">
        <div className="card-title">Ventes sur la parcelle <span>DVF</span></div>
        {!b?.idu && <div className="small muted">Parcelle non identifiée.</div>}
        {dvf.status === 'loading' && <div className="skeleton" />}
        {dvf.status === 'error' && <div className="small muted">Données de ventes indisponibles pour le moment.</div>}
        {dvf.status === 'ok' && (Array.isArray(dvf.data) && dvf.data.length ? (
          <div className="list">
            {dvf.data.map((m, i) => (
              <div key={i} className="list-row glass-in">
                <div className="main">
                  <div className="t">{eur(m.valeur_fonciere)}</div>
                  <div className="s">{[m.date_mutation && frDate(m.date_mutation), m.types_locaux?.join(', ') || m.nature_mutation,
                    m.surface_bati && `${Math.round(m.surface_bati)} m²`, m.nb_pieces && `${m.nb_pieces} pièces`].filter(Boolean).join(' · ')}</div>
                </div>
              </div>
            ))}
          </div>
        ) : <div className="small muted">Aucune vente trouvée sur cette parcelle.</div>)}
      </div>
    </>
  );
}
