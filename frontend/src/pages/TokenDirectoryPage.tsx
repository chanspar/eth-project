import { useEffect, useState } from 'react';
import axios from 'axios';
import { BookOpen, Loader2 } from 'lucide-react';

const API_BASE = 'http://localhost:8000';

interface Token {
  address: string;
  symbol: string | null;
  name: string | null;
  decimals: number | null;
}

const ALPHABET = '#ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');

export default function TokenDirectoryPage() {
  const [tokens, setTokens] = useState<Token[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activePrefix, setActivePrefix] = useState<string>('A');

  useEffect(() => {
    let active = true;

    async function fetchTokens() {
      setLoading(true);
      setError(null);
      try {
        let prefixParam = activePrefix === '#' ? '' : activePrefix;
        const res = await axios.get(`${API_BASE}/api/v1/tokens/?limit=100&offset=0&prefix=${encodeURIComponent(prefixParam)}`);
        
        // If # is selected, we filter out A-Z locally since the backend prefix filter matches 'starts with'
        let data: Token[] = res.data;
        if (activePrefix === '#') {
           data = data.filter(t => t.symbol && !/^[A-Za-z]/.test(t.symbol));
        }

        if (active) {
          setTokens(data);
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
    return () => { active = false; };
  }, [activePrefix]);

  return (
    <main className="app-layout">
      <div style={{ marginBottom: 32, marginTop: 48 }}>
        <h1 style={{
          fontSize: 40,
          fontWeight: 600,
          letterSpacing: '-1px',
          lineHeight: 1.15,
          margin: '0 0 8px 0',
          color: 'var(--ink)',
          display: 'flex',
          alignItems: 'center',
          gap: '12px'
        }}>
          <BookOpen size={40} />
          Token Directory
        </h1>
        <p style={{
          fontSize: 18,
          color: 'var(--ink-subtle)',
          letterSpacing: '-0.1px',
        }}>
          Browse all registered ERC-20 tokens alphabetically.
        </p>
      </div>

      <div style={{
        display: 'flex',
        flexWrap: 'wrap',
        gap: '8px',
        marginBottom: '32px'
      }}>
        {ALPHABET.map(letter => (
          <button
            key={letter}
            onClick={() => setActivePrefix(letter)}
            style={{
              width: '36px',
              height: '36px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              borderRadius: '8px',
              border: activePrefix === letter ? 'none' : '1px solid var(--border)',
              background: activePrefix === letter ? '#3B82F6' : 'transparent',
              color: activePrefix === letter ? 'white' : 'var(--ink)',
              fontWeight: 500,
              cursor: 'pointer',
              transition: 'all 0.2s'
            }}
          >
            {letter}
          </button>
        ))}
      </div>

      <section className="card full-width">
        <div className="card-header">
          <h2 className="card-title">
            Tokens starting with "{activePrefix}"
          </h2>
        </div>

        {loading ? (
          <div style={{ padding: '40px', display: 'flex', justifyContent: 'center' }}>
            <Loader2 className="spin-icon" size={32} color="#3B82F6" />
          </div>
        ) : error ? (
          <div className="empty-state">
            <p className="error-text">{error}</p>
          </div>
        ) : tokens.length > 0 ? (
          <div className="token-list">
            {tokens.map((token) => (
              <div key={token.address} className="token-list-item" style={{
                display: 'flex',
                justifyContent: 'space-between',
                padding: '16px',
                borderBottom: '1px solid var(--border)'
              }}>
                <div>
                  <div style={{ fontWeight: 600, color: 'var(--ink)' }}>{token.symbol || 'Unknown'}</div>
                  <div style={{ fontSize: 13, color: 'var(--ink-subtle)' }}>{token.name || 'Unknown Token'}</div>
                </div>
                <div style={{ textAlign: 'right' }}>
                  <div style={{ fontSize: 13, fontFamily: 'monospace', color: 'var(--ink-tertiary)' }}>
                    {token.address}
                  </div>
                  <div style={{ fontSize: 12, color: 'var(--ink-tertiary)' }}>
                    Decimals: {token.decimals}
                  </div>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="empty-state">
            <p>No tokens found starting with "{activePrefix}".</p>
          </div>
        )}
      </section>
    </main>
  );
}
