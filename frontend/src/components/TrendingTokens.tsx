import { useEffect, useState } from 'react';
import { TrendingUp } from 'lucide-react';

const API_BASE = 'http://localhost:8000';

interface TokenData {
  rank: number;
  symbol: string;
  name: string;
  address: string;
  transfer_count: number;
}

interface TrendingResponse {
  tokens: TokenData[];
  period_hours: number;
}

const TIME_OPTIONS = [
  { label: '1H', hours: 1 },
  { label: '6H', hours: 6 },
  { label: '24H', hours: 24 },
];

function truncateAddress(addr: string): string {
  if (!addr || addr.length < 12) return addr;
  return `${addr.slice(0, 6)}…${addr.slice(-4)}`;
}

export default function TrendingTokens() {
  const [hours, setHours] = useState(1);
  const [data, setData] = useState<TrendingResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    setLoading(true);

    async function fetchTokens() {
      try {
        const res = await fetch(
          `${API_BASE}/api/v1/tokens/trending?limit=10&hours=${hours}`
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const json = await res.json();
        if (active) {
          setData(json);
          setError(null);
          setLoading(false);
        }
      } catch (err) {
        if (active) {
          setError(err instanceof Error ? err.message : 'Failed to fetch tokens');
          setLoading(false);
        }
      }
    }

    fetchTokens();
    const interval = setInterval(fetchTokens, 30_000);

    return () => {
      active = false;
      clearInterval(interval);
    };
  }, [hours]);

  return (
    <section className="card full-width" id="trending-tokens">
      <div className="card-header">
        <h2 className="card-title">
          <TrendingUp />
          Trending Tokens
        </h2>

        <div className="token-tabs" role="tablist">
          {TIME_OPTIONS.map((opt) => (
            <button
              key={opt.hours}
              className={`token-tab ${hours === opt.hours ? 'active' : ''}`}
              onClick={() => setHours(opt.hours)}
              role="tab"
              aria-selected={hours === opt.hours}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {loading ? (
        <div className="loading-shimmer" style={{ height: 200, width: '100%' }} />
      ) : error ? (
        <div className="empty-state">
          <p className="error-text">{error}</p>
        </div>
      ) : !data || data.tokens.length === 0 ? (
        <div className="empty-state">
          <p>No trending tokens found in this period.</p>
        </div>
      ) : (
        <table className="token-table">
          <thead>
            <tr>
              <th className="token-rank">Rank</th>
              <th>Token</th>
              <th>Contract Address</th>
              <th className="token-volume">Transfers</th>
            </tr>
          </thead>
          <tbody>
            {data.tokens.map((token, index) => (
              <tr key={token.address}>
                <td className="token-rank">{index + 1}</td>
                <td>
                  <span className="token-symbol">{token.symbol}</span>
                  <span className="token-name">{token.name}</span>
                </td>
                <td className="token-address">
                  <span>{truncateAddress(token.address)}</span>
                </td>
                <td className="token-volume">{token.transfer_count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}