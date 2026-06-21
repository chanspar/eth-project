import { useEffect, useState } from 'react';
import { Fuel } from 'lucide-react';

const API_BASE = 'http://localhost:8000';

interface GasData {
  avg_gas_gwei: number;
  block_number: number | null;
  measured_at: string | null;
}

export default function GasTracker() {
  const [data, setData] = useState<GasData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;

    async function fetchGas() {
      try {
        const res = await fetch(`${API_BASE}/api/v1/metrics/gas`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const json = await res.json();
        if (active) {
          setData(json);
          setError(null);
          setLoading(false);
        }
      } catch (err) {
        if (active) {
          setError(err instanceof Error ? err.message : 'Failed to fetch gas data');
          setLoading(false);
        }
      }
    }

    fetchGas();
    const interval = setInterval(fetchGas, 10_000);

    return () => {
      active = false;
      clearInterval(interval);
    };
  }, []);

  return (
    <section className="card" id="gas-tracker">
      <div className="card-header">
        <h2 className="card-title">
          <Fuel />
          Gas Price
        </h2>
        {data?.measured_at && (
          <span style={{ fontSize: 11, color: 'var(--ink-tertiary)' }}>
            Updated {new Date(data.measured_at).toLocaleTimeString()}
          </span>
        )}
      </div>

      {loading ? (
        <div className="loading-shimmer" style={{ height: 72, width: '60%' }} />
      ) : error ? (
        <div className="empty-state">
          <p className="error-text">{error}</p>
        </div>
      ) : data ? (
        <>
          <div>
            <span className="gas-value">
              {data.avg_gas_gwei.toFixed(2)}
            </span>
            <span className="gas-unit">Gwei</span>
          </div>
          <div className="gas-meta">
            <div>
              Block: <span>{data.block_number ?? 'N/A'}</span>
            </div>
          </div>
        </>
      ) : (
        <div className="empty-state">
          <p>No gas data available.</p>
        </div>
      )}
    </section>
  );
}