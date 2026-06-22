import { Fish, ArrowUpRight } from 'lucide-react';
import type { WhaleAlert } from '../hooks/useWhaleWebSocket';

interface WhaleAlertsProps {
  messages: WhaleAlert[];
}

function formatEth(value: number): string {
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toFixed(2);
}

function timeAgo(timestamp: string): string {
  // Ensure the timestamp is parsed as UTC if it lacks a timezone specifier
  let ts = timestamp;
  if (!ts.endsWith('Z') && !ts.includes('+')) {
    ts += 'Z';
  }
  const diff = Date.now() - new Date(ts).getTime();
  const seconds = Math.max(0, Math.floor(diff / 1000));
  if (seconds < 60) return `Just now`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ago`;
}

export default function WhaleAlerts({ messages }: WhaleAlertsProps) {
  return (
    <section className="card" id="whale-alerts">
      <div className="card-header">
        <h2 className="card-title">
          <Fish />
          Whale Alerts
        </h2>
        <span style={{ fontSize: 12, color: 'var(--ink-tertiary)' }}>
          {messages.length} transactions
        </span>
      </div>

      {messages.length === 0 ? (
        <div className="empty-state">
          <Fish />
          <p>Waiting for whale transactions…</p>
          <p style={{ fontSize: 12 }}>Large transfers (&gt;100 ETH) will appear here in real time.</p>
        </div>
      ) : (
        <div className="whale-list">
          {messages.map((alert) => (
            <div className="whale-item" key={alert.id}>
              <div className="whale-icon outgoing">
                <ArrowUpRight size={16} />
              </div>

              <div className="whale-addresses">
                <div>
                  {alert.from_label ? (
                    <span className="whale-label">From: {alert.from_label}</span>
                  ) : (
                    <span className="whale-addr">From: {alert.from_address}</span>
                  )}
                </div>
                <div>
                  {alert.to_label ? (
                    <span className="whale-label">To: {alert.to_label}</span>
                  ) : (
                    <span className="whale-addr">To: {alert.to_address}</span>
                  )}
                </div>
              </div>

              <div className="whale-value">
                {formatEth(alert.value_eth)} ETH
              </div>

              <div className="whale-time">
                {alert.timestamp ? timeAgo(alert.timestamp) : 'Just now'}
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}