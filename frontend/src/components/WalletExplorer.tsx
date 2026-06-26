import { useEffect, useState, useCallback } from 'react';
import { Wallet, Copy, Check, Search } from 'lucide-react';

const API_BASE = 'http://localhost:8000';
const DEFAULT_ADDRESS = '0xd8da6bf26964af9d7eed9e03e53415d37aa96045';
const ADDRESS_REGEX = /^0x[a-fA-F0-9]{40}$/;

interface WalletTx {
  hash: string;
  from_address: string;
  to_address: string;
  value_eth?: number;
  value?: number;
  symbol?: string;
  timestamp: string;
}

interface WalletHistoryResponse {
  address: string;
  eth_transactions: WalletTx[];
  token_transfers: WalletTx[];
}

function truncateAddress(addr: string): string {
  if (!addr || addr.length < 12) return addr;
  return `${addr.slice(0, 6)}…${addr.slice(-4)}`;
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // Clipboard API not available
    }
  };

  return (
    <button className="copy-btn" onClick={handleCopy} title="Copy address">
      {copied ? <Check size={12} /> : <Copy size={12} />}
    </button>
  );
}

export default function WalletExplorer() {
  const [input, setInput] = useState(DEFAULT_ADDRESS);
  const [searchedAddress, setSearchedAddress] = useState('');
  const [data, setData] = useState<WalletHistoryResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const isValid = ADDRESS_REGEX.test(input);

  const fetchHistory = useCallback(async (address: string) => {
    setLoading(true);
    setError(null);
    setSearchedAddress(address);

    try {
      const res = await fetch(
        `${API_BASE}/api/v1/wallets/${address}/history?limit=10`
      );
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setData(json);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch history');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchHistory(DEFAULT_ADDRESS);
  }, [fetchHistory]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (isValid) {
      fetchHistory(input);
    }
  };

  return (
    <section className="card full-width" id="wallet-explorer">
      <div className="card-header">
        <h2 className="card-title">
          <Wallet />
          Wallet Explorer
        </h2>
      </div>

      <form onSubmit={handleSubmit} className="wallet-search-form">
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Enter Ethereum address (0x...)"
          className={`wallet-input ${input && !isValid ? 'invalid' : ''}`}
        />
        <button
          type="submit"
          className="btn-primary"
          disabled={!isValid || loading}
        >
          <Search size={16} style={{ marginRight: 6, verticalAlign: 'middle' }} />
          Search
        </button>
      </form>

      {loading ? (
        <div className="loading-shimmer" style={{ height: 180, width: '100%' }} />
      ) : error ? (
        <div className="empty-state">
          <p className="error-text">{error}</p>
        </div>
      ) : !data || (data.eth_transactions.length === 0 && data.token_transfers.length === 0) ? (
        <div className="empty-state">
          <Wallet />
          <p>No transactions found for this address.</p>
        </div>
      ) : (
        <div className="wallet-history-list">
          {[...data.eth_transactions, ...data.token_transfers]
            .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime())
            .map((tx) => {
            const isSend = tx.from_address.toLowerCase() === searchedAddress.toLowerCase();
            const partnerAddress = isSend ? tx.to_address : tx.from_address;

            return (
              <div className="wallet-tx-item" key={tx.hash}>
                <span className={`tx-type-badge ${isSend ? 'send' : 'receive'}`}>
                  {isSend ? 'Send' : 'Receive'}
                </span>

                <div className="tx-address-col">
                  <span className="tx-address">{truncateAddress(partnerAddress)}</span>
                  <CopyButton text={partnerAddress} />
                </div>

                <span className="tx-value">
                  {tx.value_eth !== undefined ? tx.value_eth.toFixed(4) : tx.value?.toFixed(4)} {tx.symbol || 'ETH'}
                </span>

                <span className="tx-time">
                  {new Date(tx.timestamp).toLocaleTimeString()}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}