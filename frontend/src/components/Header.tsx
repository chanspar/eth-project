import type { ConnectionStatus } from '../hooks/useWhaleWebSocket';
import { Link, useLocation } from 'react-router-dom';

interface HeaderProps {
  status: ConnectionStatus;
}

const STATUS_CONFIG: Record<ConnectionStatus, { label: string; className: string }> = {
  live: { label: 'Live', className: 'live' },
  reconnecting: { label: 'Reconnecting', className: 'reconnecting' },
  offline: { label: 'Offline', className: 'offline' },
};

export default function Header({ status }: HeaderProps) {
  const { label, className } = STATUS_CONFIG[status];
  const location = useLocation();

  return (
    <header className="top-nav">
      <div className="nav-brand">
        <span className="diamond">⟠</span>
        <span>Ethereum Dashboard</span>
      </div>

      <nav className="nav-links" style={{ display: 'flex', gap: '32px', marginLeft: '48px', flex: 1, alignItems: 'center' }}>
        <Link to="/" style={{ color: location.pathname === '/' ? 'var(--ink)' : 'var(--ink-subtle)', textDecoration: 'none', fontWeight: location.pathname === '/' ? 600 : 500, transition: 'color 0.2s' }}>Dashboard</Link>
        <Link to="/directory" style={{ color: location.pathname.startsWith('/directory') ? 'var(--ink)' : 'var(--ink-subtle)', textDecoration: 'none', fontWeight: location.pathname.startsWith('/directory') ? 600 : 500, transition: 'color 0.2s' }}>Token Directory</Link>
      </nav>

      <div className="status-pill" id="connection-status">
        <span className={`status-dot ${className}`} />
        {label}
      </div>
    </header>
  );
}
