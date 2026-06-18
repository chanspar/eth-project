import Header from './components/Header';
import GasTracker from './components/GasTracker';
import WhaleAlerts from './components/WhaleAlerts';
import TrendingTokens from './components/TrendingTokens';
import WalletExplorer from './components/WalletExplorer';
import { useWhaleWebSocket } from './hooks/useWhaleWebSocket';

const WS_URL = 'ws://localhost:8000/ws/whales';

function App() {
  const { messages, status } = useWhaleWebSocket(WS_URL);

  return (
    <>
      <Header status={status} />

      <main className="app-layout">
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
          marginBottom: 32,
          letterSpacing: '-0.1px',
        }}>
          Live Ethereum network monitoring — gas, whales, tokens, and wallets.
        </p>

        <div className="dashboard-grid">
          <GasTracker />
          <WhaleAlerts messages={messages} />
          <TrendingTokens />
          <WalletExplorer />
        </div>
      </main>
    </>
  );
}

export default App;
