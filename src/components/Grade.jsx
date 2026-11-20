import { DPE_HEX, DPE_TEXT, GRADES } from '../lib/config.js';

// Étiquette DPE pleine. `from` : ancienne étiquette (affichée « F→E »).
export function Grade({ g, size = 'sm', from, hatch, label }) {
  if (!DPE_HEX[g]) {
    return <span className={`grade ${size} none`} aria-label={label || 'Étiquette non disponible'}>–</span>;
  }
  return (
    <span className={`grade ${size}${hatch ? ' hatch' : ''}`} style={{ background: DPE_HEX[g], color: DPE_TEXT[g] }}
      aria-label={label || (from ? `Étiquette ${g} (anciennement ${from})` : `Étiquette ${g}`)}>
      {from ? `${from}→${g}` : g}
    </span>
  );
}

export function Scale({ active, small }) {
  return (
    <div className="scale" role="img" aria-label={active ? `Étiquette ${active} sur une échelle de A à G` : 'Échelle des étiquettes DPE de A à G'}>
      {GRADES.map(g => (
        <span key={g} aria-hidden="true" className={`grade ${g === active ? 'lg' : 'xs'}`}
          style={{ background: DPE_HEX[g], color: DPE_TEXT[g], ...(small && g !== active ? { minWidth: 20, height: 20, fontSize: 11 } : null) }}>{g}</span>
      ))}
    </div>
  );
}
