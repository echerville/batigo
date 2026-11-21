import { useMemo, useRef, useState } from 'react';
import Icon from './Icon.jsx';
import { Grade } from './Grade.jsx';
import { SCORE_BAS, isReplaced } from '../lib/mesdpe.js';

const key = c => (c || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/[-\s]+/g, ' ').trim();
const fmt = d => (d ? d.split('-').reverse().join('/') : '');

export default function MesDpe({ list, busy, show, onShow, onImport, onRefresh, onGo, onClear }) {
  const input = useRef(null);
  const [commune, setCommune] = useState('');
  const [year, setYear] = useState('');
  const [hideOld, setHideOld] = useState(true);

  const communes = useMemo(() => {
    const m = new Map(); // clé sans accents ni majuscules → nom affiché (la graphie non tout en majuscules est préférée)
    for (const x of list) if (x.commune && (!m.has(key(x.commune)) || m.get(key(x.commune)) === m.get(key(x.commune)).toUpperCase())) m.set(key(x.commune), x.commune);
    return [...m.entries()].sort((a, b) => a[1].localeCompare(b[1], 'fr'));
  }, [list]);
  const years = useMemo(() => [...new Set(list.map(x => (x.date || '').slice(0, 4)).filter(Boolean))].sort().reverse(), [list]);
  const shown = list.filter(x => (!commune || key(x.commune) === commune) && (!year || (x.date || '').startsWith(year)) && !(hideOld && isReplaced(x)));
  const pending = list.filter(x => x.etat === 'attente' || x.etat === 'erreur').length;
  const nOld = list.filter(isReplaced).length;

  return (
    <div>
      <input ref={input} type="file" accept=".csv,.xlsx,.xls,.xml,text/csv,text/xml" hidden
        onChange={e => { const f = e.target.files?.[0]; e.target.value = ''; if (f) onImport(f); }} />
      <button type="button" className="btn ghost rep-export" style={{ marginTop: 0 }} disabled={!!busy} onClick={() => input.current.click()}>
        <Icon name="download" size={16} /> Importer mon export ADEME (CSV, XLSX, XML)
      </button>
      {busy && (
        <div role="status" className="small muted" style={{ marginTop: 6 }}>
          {busy.msg}
          {busy.total > 0 && <div className="dpe-progress"><span style={{ width: `${Math.round((busy.done / busy.total) * 100)}%` }} /></div>}
        </div>
      )}

      {list.length === 0 && !busy ? (
        <div className="small muted rep-empty">Aucun DPE importé. Exportez la liste de vos DPE depuis votre espace ADEME, puis importez le fichier : Scope retrouve l’adresse de chacun et les affiche ici et sur la carte.</div>
      ) : (
        <>
          <label className="dpe-show small"><input type="checkbox" checked={show} onChange={e => onShow(e.target.checked)} /> Afficher mes DPE sur la carte</label>
          <div className="dpe-filters">
            <select value={commune} onChange={e => setCommune(e.target.value)} aria-label="Commune">
              <option value="">Toutes les communes</option>
              {communes.map(([k, c]) => <option key={k} value={k}>{c}</option>)}
            </select>
            <select value={year} onChange={e => setYear(e.target.value)} aria-label="Année">
              <option value="">Toutes les années</option>
              {years.map(y => <option key={y} value={y}>{y}</option>)}
            </select>
          </div>
          {nOld > 0 && <label className="dpe-show small"><input type="checkbox" checked={hideOld} onChange={e => setHideOld(e.target.checked)} /> Masquer les DPE remplacés ({nOld})</label>}
          <div className="small muted" style={{ marginBottom: 4 }}>{shown.length} DPE</div>
          <div className="rep-list">
            {shown.map(x => {
              const can = x.etat === 'ok' && x.lat != null;
              const low = x.score != null && x.score < SCORE_BAS;
              return (
                <button key={x.n} type="button" className={`dpe-row${isReplaced(x) ? ' is-replaced' : ''}`} disabled={!can} onClick={() => onGo(x)}
                  aria-label={`${x.adresse || x.n}${x.etiquette ? `, étiquette ${x.etiquette}` : ''}`}>
                  <Grade g={x.etiquette} />
                  <span className="dpe-txt">
                    <div><strong>{x.etat === 'ok' ? (low && x.brut ? x.brut : x.adresse) : x.n}</strong></div>
                    <div className="muted">
                      {x.commune}{x.commune && x.date ? ' · ' : ''}{fmt(x.date)}
                      {x.ref ? ` · ${x.ref}` : ''}
                    </div>
                    <div>
                      {x.etat === 'attente' && <span className="dpe-flag">En attente de publication</span>}
                      {x.etat === 'erreur' && <span className="dpe-flag warn">Service ADEME indisponible</span>}
                      {x.etat === 'tertiaire' && <span className="dpe-flag">Tertiaire : à part</span>}
                      {isReplaced(x) && <span className="dpe-flag">Remplacé</span>}
                      {low && <span className="dpe-flag warn">Adresse à vérifier</span>}
                    </div>
                  </span>
                </button>
              );
            })}
          </div>
          {pending > 0 && <button type="button" className="btn ghost rep-export" disabled={!!busy} onClick={onRefresh}>Réessayer les {pending} DPE en attente</button>}
          <button type="button" className="btn ghost rep-export" disabled={!!busy} onClick={onClear}><Icon name="trash" size={16} /> Vider ma liste</button>
        </>
      )}
      <div className="small muted rep-foot">Privés : enregistrés sur cet appareil, jamais partagés. Pas de nom de client.</div>
    </div>
  );
}
