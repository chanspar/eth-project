import { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import { Search, Loader2, Coins } from 'lucide-react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip as ChartTooltip,
  Filler,
  Legend,
} from 'chart.js';
import { Line } from 'react-chartjs-2';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  ChartTooltip,
  Filler,
  Legend
);

const API_BASE = 'http://localhost:8000';

interface TokenSearchData {
  address: string;
  symbol: string;
  name: string;
  decimals: number;
}

interface TrendData {
  time_bucket: string;
  transfer_count: number;
  total_value: number;
}

function truncateAddress(addr: string): string {
  if (!addr) return '';
  return `${addr.slice(0, 6)}…${addr.slice(-4)}`;
}

function formatVolume(value: number): string {
  if (!value) return '0';
  if (value >= 1e9) return (value / 1e9).toFixed(2) + ' B';
  if (value >= 1e6) return (value / 1e6).toFixed(2) + ' M';
  if (value >= 1e3) return (value / 1e3).toFixed(2) + ' K';
  return value.toFixed(2);
}

export default function TokenExplorer() {
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<TokenSearchData[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [hasSearched, setHasSearched] = useState(false);
  const [selectedToken, setSelectedToken] = useState<TokenSearchData | null>(null);
  const [trends, setTrends] = useState<TrendData[]>([]);
  const [isLoadingTrends, setIsLoadingTrends] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState<number>(-1);

  const dropdownRef = useRef<HTMLDivElement>(null);
  const dropdownListRef = useRef<HTMLDivElement>(null);

  // Scroll into view when keyboard navigation is used
  useEffect(() => {
    if (dropdownListRef.current && selectedIndex >= 0) {
      const selectedEl = dropdownListRef.current.children[selectedIndex] as HTMLElement;
      if (selectedEl) {
        selectedEl.scrollIntoView({ block: 'nearest' });
      }
    }
  }, [selectedIndex]);

  // Debounced search
  useEffect(() => {
    if (searchQuery.length < 2) {
      setSearchResults([]);
      setHasSearched(false);
      return;
    }

    const delayDebounceFn = setTimeout(async () => {
      setIsSearching(true);
      try {
        const res = await axios.get(`${API_BASE}/api/v1/tokens/search?q=${encodeURIComponent(searchQuery)}`);
        setSearchResults(res.data);
        setHasSearched(true);
      } catch (error) {
        console.error('Search failed', error);
        setSearchResults([]);
      } finally {
        setIsSearching(false);
      }
    }, 500);

    return () => clearTimeout(delayDebounceFn);
  }, [searchQuery]);

  // Click outside to close dropdown
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setSearchResults([]);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [dropdownRef]);

  const selectToken = async (token: TokenSearchData) => {
    setSelectedToken(token);
    setSearchResults([]);
    setSearchQuery('');
    
    setIsLoadingTrends(true);
    try {
      const res = await axios.get(`${API_BASE}/api/v1/tokens/${token.address}/trends`);
      setTrends(res.data.trends || []);
    } catch (error) {
      console.error('Failed to load trends', error);
    } finally {
      setIsLoadingTrends(false);
    }
  };

  const totalTransfers24h = trends.reduce((acc, curr) => acc + curr.transfer_count, 0);
  const totalVolume24h = trends.reduce((acc, curr) => acc + curr.total_value, 0);

  const chartData = {
    labels: trends.map(t => {
      const d = new Date(t.time_bucket);
      return `${d.getHours()}:00`;
    }),
    datasets: [
      {
        label: 'Transfer Count',
        data: trends.map(t => t.transfer_count),
        borderColor: '#60A5FA',
        backgroundColor: 'rgba(96, 165, 250, 0.1)',
        borderWidth: 2,
        pointBackgroundColor: '#3B82F6',
        pointBorderColor: '#1E3A8A',
        fill: true,
        tension: 0.4
      }
    ]
  };

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: {
      intersect: false,
      mode: 'index' as const,
    },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: 'rgba(17, 24, 39, 0.9)',
        titleColor: '#F3F4F6',
        bodyColor: '#D1D5DB',
        borderColor: '#374151',
        borderWidth: 1,
        padding: 10
      }
    },
    scales: {
      x: {
        grid: { color: '#374151' },
        ticks: { color: '#9CA3AF' },
        border: { display: false }
      },
      y: {
        grid: { color: '#374151' },
        ticks: { color: '#9CA3AF' },
        border: { display: false },
        beginAtZero: true
      }
    }
  };

  return (
    <section className="card full-width" id="token-explorer">
      <div className="card-header">
        <h2 className="card-title">
          <Coins />
          Token Explorer
        </h2>
      </div>

      <div className="token-search-container" ref={dropdownRef}>
        <div className="token-search-input-wrapper">
          <Search size={20} className="search-icon" />
          <input
            type="text"
            className="token-search-input"
            placeholder="Search by token name or symbol (e.g. USDT)..."
            value={searchQuery}
            onChange={(e) => {
              setSearchQuery(e.target.value);
              setSelectedIndex(-1);
            }}
            onKeyDown={(e) => {
              if (e.key === 'ArrowDown') {
                e.preventDefault();
                setSelectedIndex(prev => Math.min(prev + 1, searchResults.length - 1));
              } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                setSelectedIndex(prev => Math.max(prev - 1, -1));
              } else if (e.key === 'Enter') {
                e.preventDefault();
                if (selectedIndex >= 0 && searchResults[selectedIndex]) {
                  selectToken(searchResults[selectedIndex]);
                } else if (searchResults.length > 0) {
                  selectToken(searchResults[0]);
                }
              }
            }}
          />
        </div>

        {searchResults.length > 0 && (
          <div className="token-search-dropdown" ref={dropdownListRef}>
            {searchResults.map((token, index) => (
              <div 
                key={token.address} 
                className="token-search-item"
                style={{ backgroundColor: index === selectedIndex ? 'rgba(59, 130, 246, 0.15)' : 'transparent' }}
                onClick={() => selectToken(token)}
                onMouseEnter={() => setSelectedIndex(index)}
              >
                <div className="token-search-item-info">
                  <span className="token-symbol">{token.symbol}</span>
                  <span className="token-name">{token.name}</span>
                </div>
                <span className="token-address-mini">{truncateAddress(token.address)}</span>
              </div>
            ))}
          </div>
        )}

        {isSearching && (
          <div className="token-search-msg">
            <Loader2 size={16} className="spin-icon" /> Searching...
          </div>
        )}

        {searchQuery.length >= 2 && searchResults.length === 0 && !isSearching && hasSearched && (
          <div className="token-search-msg">No tokens found.</div>
        )}
      </div>

      {selectedToken && (
        <div className="token-details-card">
          <div className="token-details-header">
            <div>
              <h3 className="token-details-title">
                <span className="token-symbol-large">{selectedToken.symbol}</span>
                <span className="token-name-large">{selectedToken.name}</span>
              </h3>
              <p className="token-address-full">{selectedToken.address}</p>
            </div>
            <div className="token-decimals">
              <span className="token-decimals-label">Decimals</span>
              <span className="token-decimals-value">{selectedToken.decimals || 'Unknown'}</span>
            </div>
          </div>

          <div className="token-stats-grid">
            <div className="token-stat-box">
              <div className="token-stat-label">Total Transfers (24h)</div>
              <div className="token-stat-value">{totalTransfers24h.toLocaleString()}</div>
            </div>
            <div className="token-stat-box">
              <div className="token-stat-label">Total Volume (24h)</div>
              <div className="token-stat-value highlight-green">{formatVolume(totalVolume24h)}</div>
            </div>
          </div>

          <div className="token-chart-section">
            <h4 className="token-chart-title">Transfer Activity Trends (Last 24h)</h4>
            <div className="token-chart-container">
              <Line data={chartData} options={chartOptions} />
            </div>
            {isLoadingTrends && (
              <div className="token-chart-loading">
                <Loader2 size={24} className="spin-icon" />
                <span>Loading time-series data from TimescaleDB...</span>
              </div>
            )}
          </div>
        </div>
      )}
    </section>
  );
}
