// Logos des couches : symbologie réglementaire (étiquette DPE, plan cadastral, zonage CNIG…)
const S = { width: 30, height: 30, viewBox: '1 1 28 28', 'aria-hidden': true };

const ICONS = {
  // DPE : une maison remplie des 7 couleurs de l'étiquette, du A (vert) au G (rouge)
  dpe: (
    <svg {...S}>
      <defs><clipPath id="li-house"><path d="M15 2.5L28 14H24.5V27.5H5.5V14H2Z" /></clipPath></defs>
      <g clipPath="url(#li-house)">
        {['#009C6D', '#52B153', '#78BD76', '#F4E70F', '#F0B50F', '#EB8235', '#D7221F'].map((c, i) => (
          <rect key={c} x="0" y={2.5 + i * 3.6} width="30" height="3.7" fill={c} />
        ))}
      </g>
    </svg>
  ),
  // Plan cadastral : limites de parcelles et bâti hachuré
  cadastre: (
    <svg {...S}>
      <rect x="1" y="1" width="28" height="28" rx="3" fill="#FFFFFF" />
      <path d="M12 10.5L21.5 11.5L22 20.5L11.5 20Z" fill="#F7A823" />
      <path d="M1 12L29 10M1 21L29 22M12 1L11 29M21 1L22 29" stroke="#111" strokeWidth="0.8" fill="none" />
    </svg>
  ),
  // Réseau de chaleur : conduite et sous-stations
  rcu: (
    <svg {...S}>
      <rect x="1" y="1" width="28" height="28" rx="3" fill="#FFF1E6" />
      <path d="M4 22H12V9H26" stroke="#E05C00" strokeWidth="3" fill="none" strokeLinejoin="round" />
      <circle cx="12" cy="22" r="2.6" fill="#fff" stroke="#E05C00" strokeWidth="1.6" />
      <circle cx="26" cy="9" r="2.6" fill="#fff" stroke="#E05C00" strokeWidth="1.6" />
    </svg>
  ),
  // Gaz : flamme sur conduite
  gaz: (
    <svg {...S}>
      <rect x="1" y="1" width="28" height="28" rx="3" fill="#FFF7D6" />
      <path d="M3 24H27" stroke="#D4A000" strokeWidth="3" />
      <path d="M15 5C18 9 20 11 20 15A5 5 0 0 1 10 15C10 12.5 11.5 11 12.5 9.5C13 11.5 14 12 14.8 12C14.5 9.5 14 7.5 15 5Z" fill="#1F6FD1" />
    </svg>
  ),
  // Zonage CNIG : U, AU, A, N
  plu: (
    <svg {...S}>
      <rect x="1" y="1" width="28" height="28" rx="6" fill="#2B5BA8" />
      <path d="M15 4.5V8.5" stroke="#fff" strokeWidth="1.2" />
      <path d="M15 4.7H19V7H15Z" fill="#E63946" />
      <path d="M5.5 13L15 8.5L24.5 13Z" fill="#fff" />
      <circle cx="15" cy="11.4" r="1.3" fill="#2B5BA8" />
      <path d="M7.5 14.5H22.5V16H7.5Z M9 16.5H11V22H9Z M14 16.5H16V22H14Z M19 16.5H21V22H19Z M6.5 22.5H23.5V24.5H6.5Z" fill="#fff" />
    </svg>
  ),
  // Monuments historiques : disque orange quadrillé, comme sur la carte
  ac1: (
    <svg {...S}>
      <defs>
        <pattern id="ac1-h" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
          <path d="M0 0V6" stroke="#FF8000" strokeWidth="1.6" />
          <path d="M0 3H6" stroke="#FF8000" strokeWidth="1.6" />
        </pattern>
      </defs>
      <circle cx="15" cy="15" r="12.5" fill="#fff" />
      <circle cx="15" cy="15" r="12.5" fill="url(#ac1-h)" stroke="#FF8000" strokeWidth="1.8" />
    </svg>
  ),
  // Site classé ou inscrit : paysage protégé
  ac2: (
    <svg {...S}>
      <defs>
        <pattern id="ac2-h" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
          <path d="M0 0V6" stroke="#7B2FBE" strokeWidth="1.6" />
          <path d="M0 3H6" stroke="#7B2FBE" strokeWidth="1.6" />
        </pattern>
      </defs>
      <path d="M4 9L13 3L26 7L27 20L17 27L5 22Z" fill="#fff" />
      <path d="M4 9L13 3L26 7L27 20L17 27L5 22Z" fill="url(#ac2-h)" stroke="#7B2FBE" strokeWidth="1.8" strokeLinejoin="round" />
    </svg>
  ),
};

export default function LayerIcon({ id }) {
  return <span className="layer-sw">{ICONS[id]}</span>;
}
