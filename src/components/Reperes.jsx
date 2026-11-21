import { useState } from 'react';
import Icon from './Icon.jsx';
import { DEFAULT_COLOR, REPERE_COLORS, REPERE_ICONS, checkText, iconSvg, loadModeles, newId, saveModeles } from '../lib/reperes.js';

const MAX = 280, MAX_NAME = 40;

function RepIcon({ id, color = DEFAULT_COLOR, size = 20 }) {
  return <span className="rep-ico" style={{ background: color }} dangerouslySetInnerHTML={{ __html: iconSvg(id, size, '#fff') }} />;
}

function Editor({ value, onSave, onDelete, onCancel }) {
  const [name, setName] = useState(value.name || '');
  const [icon, setIcon] = useState(value.icon || 'pin');
  const [color, setColor] = useState(value.color || DEFAULT_COLOR);
  const [text, setText] = useState(value.text || '');
  const [keep, setKeep] = useState(!value.id);
  const [modeles, setModeles] = useState(loadModeles);
  const [err, setErr] = useState(null);

  function save() {
    const e = checkText(name) || checkText(text);
    if (e) { setErr(e); return; }
    const n = name.trim() || 'Repère';
    if (keep && !modeles.some(m => m.name === n && m.icon === icon && m.color === color)) {
      const next = [...modeles, { id: newId(), name: n, icon, color }];
      saveModeles(next); setModeles(next);
    }
    onSave({ ...value, name: n, icon, color, text: text.trim() });
  }
  function removeModele(id) {
    const next = modeles.filter(m => m.id !== id);
    saveModeles(next); setModeles(next);
  }
  return (
    <div className="rep-editor">
      <div className="rep-kind small muted">{value.type === 'zone' ? 'Zone' : 'Point'}{value.id ? ' · modifier' : ' · nouveau'}</div>

      {modeles.length > 0 && (
        <>
          <h3>Mes modèles</h3>
          <div className="rep-models">
            {modeles.map(m => (
              <span key={m.id} className="rep-model">
                <button type="button" onClick={() => { setName(m.name); setIcon(m.icon); setColor(m.color); }} aria-label={`Utiliser le modèle ${m.name}`}>
                  <RepIcon id={m.icon} color={m.color} size={14} /><span>{m.name}</span>
                </button>
                <button type="button" className="rep-model-x" onClick={() => removeModele(m.id)} aria-label={`Retirer le modèle ${m.name}`}><Icon name="x" size={12} /></button>
              </span>
            ))}
          </div>
        </>
      )}

      <h3>Nom</h3>
      <input className="rep-name" value={name} maxLength={MAX_NAME} placeholder="Ex. Digicode, Termites, Compteur…"
        onChange={e => { setName(e.target.value); setErr(null); }} aria-label="Nom du repère" />

      <h3>Logo</h3>
      <div className="rep-grid">
        {REPERE_ICONS.map(([id, label]) => (
          <button key={id} type="button" className="rep-choice" aria-pressed={icon === id} onClick={() => setIcon(id)} title={label} aria-label={label}>
            <RepIcon id={id} color={color} size={18} />
          </button>
        ))}
      </div>

      <h3>Couleur</h3>
      <div className="rep-colors">
        {REPERE_COLORS.map(c => (
          <button key={c} type="button" className="rep-color" style={{ background: c }} aria-pressed={color === c} aria-label={`Couleur ${c}`} onClick={() => setColor(c)} />
        ))}
        <label className="rep-color rep-color-custom" title="Autre couleur" aria-label="Autre couleur">
          <input type="color" value={color} onChange={e => setColor(e.target.value)} />
        </label>
      </div>

      <h3>Note</h3>
      <textarea className="rep-text" value={text} maxLength={MAX} rows={3} placeholder="Texte libre (ni nom de client, ni coordonnées)"
        onChange={e => { setText(e.target.value); setErr(null); }} aria-label="Note du repère" />
      <div className="small muted rep-count">{text.length}/{MAX}</div>

      <label className="rep-keep small"><input type="checkbox" checked={keep} onChange={e => setKeep(e.target.checked)} /> Garder comme modèle</label>
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
            <div className="small muted rep-empty">Aucun repère pour l’instant. Posez un point ou dessinez une zone sur la carte, puis choisissez son nom, son logo et sa couleur.</div>
          ) : (
            <div className="rep-list">
              {[...list].reverse().map(r => (
                <div key={r.id} className="rep-row">
                  <button type="button" className="rep-go" onClick={() => onGo(r)} aria-label={`Voir sur la carte : ${r.name || 'Repère'}`}>
                    <RepIcon id={r.icon} color={r.color} size={16} />
                    <span className="rep-label"><strong>{r.name || 'Repère'}</strong>{r.type === 'zone' ? ' · zone' : ''}<span className="muted">{r.text ? ` — ${r.text}` : ''}</span></span>
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
