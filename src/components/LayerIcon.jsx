// Logos des couches : symbologie réglementaire (étiquette DPE, plan cadastral, zonage CNIG…)
const S = { width: 30, height: 30, viewBox: '0 0 30 30', 'aria-hidden': true };

const ICONS = {
  // Étiquette énergie : flèches A à G
  dpe: (
    <svg {...S}>
      {['#009C6D', '#52B153', '#78BD76', '#F4E70F', '#F0B50F', '#EB8235', '#D7221F'].map((c, i) => {
        const w = 9 + i * 2.6, y = 2.5 + i * 3.6;
        return <path key={c} d={`M3 ${y}H${w}L${w + 2} ${y + 1.5}L${w} ${y + 3}H3Z`} fill={c} />;
      })}
    </svg>
  ),
  // Plan cadastral : limites de parcelles et bâti hachuré
  cadastre: (
    <svg {...S}>
      <defs><pattern id="li-hach" width="3" height="3" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><path d="M0 0V3" stroke="#C0504D" strokeWidth="1.2" /></pattern></defs>
      <rect x="1" y="1" width="28" height="28" rx="3" fill="#FBFAF6" />
      <path d="M1 12L29 10M1 21L29 22M12 1L11 29M21 1L22 29" stroke="#1F2430" strokeWidth="1.1" fill="none" />
      <rect x="13.5" y="13" width="6" height="7" fill="url(#li-hach)" stroke="#C0504D" strokeWidth="1" />
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
      <rect x="1" y="1" width="14" height="14" fill="#F4B6B6" /><rect x="15" y="1" width="14" height="14" fill="#F7CF9B" />
      <rect x="1" y="15" width="14" height="14" fill="#F5EB8C" /><rect x="15" y="15" width="14" height="14" fill="#A9D9A2" />
      <g fontSize="8" fontWeight="800" fontFamily="Public Sans, sans-serif" textAnchor="middle" fill="#1F2430">
        <text x="8" y="11">U</text><text x="22" y="11">AU</text><text x="8" y="25">A</text><text x="22" y="25">N</text>
      </g>
    </svg>
  ),
  // Monument historique : temple dans son périmètre de 500 m
  ac1: (
    <svg {...S}>
      <circle cx="15" cy="15" r="13" fill="#FBE5E5" stroke="#B83B3B" strokeWidth="1.2" strokeDasharray="2.5 2" />
      <path d="M9 13L15 9L21 13Z M10 14H20 M11 15V20 M14 15V20 M16 15V20 M19 15V20 M9.5 21H20.5" stroke="#B83B3B" strokeWidth="1.4" fill="#B83B3B" strokeLinejoin="round" />
    </svg>
  ),
  // Site patrimonial remarquable : périmètre de centre ancien
  ac4: (
    <svg {...S}>
      <path d="M4 9L13 3L26 7L27 20L17 27L5 22Z" fill="#EEE3F8" stroke="#7B4BB0" strokeWidth="1.6" strokeLinejoin="round" />
      <path d="M10 19V13L13 10.5L16 13V19Z M16 19V11H21V19Z" fill="#7B4BB0" />
    </svg>
  ),
  // Site classé ou inscrit : paysage protégé
  ac2: (
    <svg {...S}>
      <path d="M4 9L13 3L26 7L27 20L17 27L5 22Z" fill="#E3F3E3" stroke="#2E8540" strokeWidth="1.6" strokeLinejoin="round" />
      <path d="M8 21L13 13L16 17L18.5 14L23 21Z" fill="#2E8540" />
      <circle cx="19" cy="10.5" r="1.8" fill="#2E8540" />
    </svg>
  ),
};

export default function LayerIcon({ id }) {
  return <span className="layer-sw">{ICONS[id]}</span>;
}
