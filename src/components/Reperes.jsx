import { useState } from 'react';
import Icon from './Icon.jsx';
import { REPERE_GROUPS, checkText, iconColor, iconLabel, iconSvg } from '../lib/reperes.js';

const MAX = 280;

function RepIcon({ id, size = 20 }) {
  return <span className="rep-ico" style={{ background: iconColor(id) }} dangerouslySetInnerHTML={{ __html: iconSvg(id, size, '#fff') }} />;
}

function Editor({ value, onSave, onDelete, onCancel }) {
  const [icon, setIcon] = useState(value.icon || 'pin');
  const [text, setText] = useState(value.text || '');
  const [err, setErr] = useState(null);
  function save() {
    const e = checkText(text);
    if (e) { setErr(e); return; }
    onSave({ ...value, icon, text: text.trim() });
  }
  return (
    <div className="rep-editor">
      <div className="rep-kind small muted">{value.type === 'zone' ? 'Zone' : 'Point'}{value.id ? ' · modifier' : ' · nouveau'}</div>
      {REPERE_GROUPS.map(g => (
        <div key={g.id}>
          <h3>{g.label}</h3>
          <div className="rep-grid">
            {g.icons.map(([id, label]) => (
              <button key={id} type="button" className="rep-choice" aria-pressed={icon === id} onClick={() => setIcon(id)}>
                <RepIcon id={id} size={18} /><span>{label}</span>
              </button>
            ))}
          </div>
        </div>
      ))}
      <h3>Note</h3>
      <textarea className="rep-text" value={text} maxLength={MAX} rows={3} placeholder="Texte libre (ni nom de client, ni coordonnées)"
        onChange={e => { setText(e.target.value); setErr(null); }} aria-label="Note du repère" />
      <div className="small muted rep-count">{text.length}/{MAX}</div>
      {err && <div className="rep-err" role="alert">{err}</div>}
      <div className="row rep-actions">
        {value.id && <button type="button" className="icon-btn" aria-label="Supprimer le repère" title="Supprimer" onClick={() => onDelete(value.id)}><Icon name="trash" size={18} /></button>}
        <span className="spacer" />
        <button type="button" className="btn ghost" onClick={onCancel}>Annuler</button>
        <button type="button" className="btn primary" onClick={save}>Enregistrer</button>
      </div>
    </div>
  );
}

export default function Reperes({ list, editing, onEdit, onSave, onDelete, onClose, onPoint, onZone, onGo, onExport }) {
  return (
    <div className="dropdown glass" role="dialog" aria-label="Mes repères">
      <div className="row" style={{ justifyContent: 'space-between', marginBottom: 8 }}>
        <h2 style={{ margin: 0 }}>Mes repères</h2>
        <button className="icon-btn" aria-label="Fermer" onClick={onClose}><Icon name="x" size={18} /></button>
      </div>
      {editing ? (
        <Editor key={editing.id || 'new'} value={editing} onSave={onSave} onDelete={onDelete} onCancel={() => onEdit(null)} />
      ) : (
        <>
          <div className="rep-add">
            <button type="button" className="btn ghost" onClick={onPoint}><Icon name="pin" size={18} /> Poser un point</button>
            <button type="button" className="btn ghost" onClick={onZone}><Icon name="polygon" size={18} /> Dessiner une zone</button>
          </div>
          {list.length === 0 ? (
            <div className="small muted rep-empty">Aucun repère pour l’instant. Posez un point ou dessinez une zone sur la carte, choisissez une icône et ajoutez une note.</div>
          ) : (
            <div className="rep-list">
              {[...list].reverse().map(r => (
                <div key={r.id} className="rep-row">
                  <button type="button" className="rep-go" onClick={() => onGo(r)} aria-label={`Voir sur la carte : ${iconLabel(r.icon)}`}>
                    <RepIcon id={r.icon} size={16} />
                    <span className="rep-label"><strong>{iconLabel(r.icon)}</strong>{r.type === 'zone' ? ' · zone' : ''}<span className="muted">{r.text ? ` — ${r.text}` : ''}</span></span>
                  </button>
                  <button type="button" className="icon-btn" aria-label="Modifier le repère" onClick={() => onEdit(r)}><Icon name="edit" size={16} /></button>
                </div>
              ))}
            </div>
          )}
          <div className="small muted rep-foot">Privés : enregistrés sur cet appareil, jamais partagés. Pas de nom de client ni de coordonnées dans les notes.</div>
          {list.length > 0 && <button type="button" className="btn ghost rep-export" onClick={onExport}><Icon name="download" size={16} /> Sauvegarder dans un fichier</button>}
        </>
      )}
    </div>
  );
}
