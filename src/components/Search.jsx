import { useEffect, useRef, useState } from 'react';
import Icon from './Icon.jsx';
import { searchAddress } from '../lib/data.js';

export default function Search({ value, onValue, onSelect, getCenter, mobile, tagline }) {
  const [list, setList] = useState([]);
  const [active, setActive] = useState(-1);
  const [open, setOpen] = useState(false);
  const timer = useRef(null);
  const reqId = useRef(0);
  const box = useRef(null);

  useEffect(() => {
    const h = e => { if (!box.current?.contains(e.target)) setOpen(false); };
    document.addEventListener('pointerdown', h);
    return () => document.removeEventListener('pointerdown', h);
  }, []);

  async function run(q, autoPick) {
    const id = ++reqId.current;
    try {
      const feats = await searchAddress(q, getCenter(), autoPick ? 1 : 6);
      if (id !== reqId.current) return;
      if (autoPick) { if (feats[0]) pick(feats[0]); return; }
      setList(feats); setActive(-1); setOpen(true);
    } catch { /* service d'adresse indisponible : on garde la saisie */ }
  }
  function change(v) {
    onValue(v);
    clearTimeout(timer.current);
    if (v.trim().length < 3) { setList([]); setOpen(false); return; }
    timer.current = setTimeout(() => run(v.trim()), 250);
  }
  function pick(f) {
    setOpen(false); setList([]);
    onValue(f.properties.label);
    onSelect(f);
  }
  function key(e) {
    if (e.key === 'ArrowDown') { e.preventDefault(); setOpen(true); setActive(a => Math.min(a + 1, list.length - 1)); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setActive(a => Math.max(a - 1, 0)); }
    else if (e.key === 'Escape') setOpen(false);
    else if (e.key === 'Enter') {
      e.preventDefault();
      if (open && list[active]) pick(list[active]);
      else if (open && list[0]) pick(list[0]);
      else if (value.trim().length > 2) { clearTimeout(timer.current); run(value.trim(), true); }
    }
  }

  return (
    <div className="searchbox glass" ref={box}>
      <div className="brand">
        <span className="wordmark" style={{ fontSize: mobile ? 19 : 22 }}>Scope<i /></span>
        {tagline && (
          <div className="tagline">
            <span>Une adresse, toutes les infos.</span>
            <span className="tagline-keys" aria-label="DPE, Data, Cadastre, Cartes">DPE · Data · Cadastre · Cartes</span>
          </div>
        )}
      </div>
      <div className="search-field">
        <Icon name="search" size={18} />
        <label htmlFor="q" className="sr-only">Rechercher une adresse</label>
        <input id="q" type="text" value={value} onChange={e => change(e.target.value)} onKeyDown={key}
          onFocus={() => list.length && setOpen(true)} placeholder="Rechercher une adresse" autoComplete="off"
          role="combobox" aria-expanded={open && list.length > 0} aria-controls="q-list"
          aria-activedescendant={active >= 0 ? `q-opt-${active}` : undefined} enterKeyHint="search" />
        {value && <button className="icon-btn" onClick={() => { onValue(''); setList([]); }} aria-label="Effacer la recherche"><Icon name="x" size={16} /></button>}
      </div>
      {open && list.length > 0 && (
        <ul id="q-list" role="listbox" className="suggestions glass" aria-label="Adresses proposées">
          {list.map((f, i) => (
            <li key={f.properties.id || i} role="presentation">
              <button id={`q-opt-${i}`} role="option" aria-selected={i === active} onClick={() => pick(f)}>
                {f.properties.name}<small>{f.properties.postcode} {f.properties.city}</small>
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
