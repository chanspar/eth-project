import { useEffect, useState } from 'react';
import { Fuel } from 'lucide-react';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';

interface GasData {
  average_gas_price_gwei: number;
  block_number?: number | null;
  measured_at?: string | null;
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
    <div style={{ display: 'flex', alignItems: 'baseline', gap: '8px', background: 'var(--surface-1)', padding: '12px 20px', borderRadius: 'var(--rounded-pill)', border: '1px solid var(--hairline)' }}>
      <Fuel size={18} color="var(--primary)" style={{ transform: 'translateY(3px)' }} />
      <span style={{ fontSize: 13, fontWeight: 500, color: 'var(--ink-subtle)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>Gas Price</span>
      
      <div style={{ marginLeft: '8px' }}>
        {loading ? (
          <span style={{ color: 'var(--ink-subtle)' }}>Loading...</span>
        ) : error ? (
          <span className="error-text">Error</span>
        ) : data && data.average_gas_price_gwei !== undefined ? (
          <div style={{ display: 'flex', alignItems: 'baseline', gap: '4px' }}>
            <span style={{ fontSize: 20, fontWeight: 600, color: 'var(--ink)', letterSpacing: '-0.5px' }}>
              {data.average_gas_price_gwei.toFixed(2)}
            </span>
            <span style={{ fontSize: 13, color: 'var(--ink-subtle)' }}>Gwei</span>
          </div>
        ) : (
          <span style={{ color: 'var(--ink-subtle)' }}>N/A</span>
        )}
      </div>
    </div>
  );
}