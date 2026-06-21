import type { ConnectionStatus } from '../hooks/useWhaleWebSocket';

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

  return (
    <header className="top-nav">
      <div className="nav-brand">
        <span className="diamond">⟠</span>
        <span>Ethereum Dashboard</span>
      </div>

      <div className="status-pill" id="connection-status">
        <span className={`status-dot ${className}`} />
        {label}
      </div>
    </header>
  );
}
