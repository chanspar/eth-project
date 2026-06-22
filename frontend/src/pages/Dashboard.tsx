import GasTracker from '../components/GasTracker';
import WhaleAlerts from '../components/WhaleAlerts';
import TrendingTokens from '../components/TrendingTokens';
import WalletExplorer from '../components/WalletExplorer';
import TokenExplorer from '../components/TokenExplorer';
import type { WhaleAlert } from '../hooks/useWhaleWebSocket';

interface DashboardProps {
  messages: WhaleAlert[];
}

export default function Dashboard({ messages }: DashboardProps) {
  return (
    <main className="app-layout">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 32 }}>
        <div>
          <h1 style={{
            fontSize: 40,
            fontWeight: 600,
            letterSpacing: '-1px',
            lineHeight: 1.15,
            margin: '48px 0 8px',
            color: 'var(--ink)',
          }}>
            Real-Time Dashboard
          </h1>
          <p style={{
            fontSize: 18,
            color: 'var(--ink-subtle)',
            letterSpacing: '-0.1px',
          }}>
            Live Ethereum network monitoring — gas, whales, tokens, and wallets.
          </p>
        </div>
        <div style={{ marginTop: '56px' }}>
          <GasTracker />
        </div>
      </div>

      <div className="dashboard-grid">
        <div className="full-width">
          <WhaleAlerts messages={messages} />
        </div>
        <TrendingTokens />
        <TokenExplorer />
        <div className="full-width">
          <WalletExplorer />
        </div>
      </div>
    </main>
  );
}
