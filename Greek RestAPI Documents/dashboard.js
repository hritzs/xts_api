import { useState, useEffect } from 'react';
import { useRouter } from 'next/router';

export default function Dashboard() {
    const router = useRouter();
    const [positions, setPositions] = useState([]);
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState('');

    useEffect(() => {
        const fetchPositions = async () => {
            try {
                const res = await fetch('/api/positions');
                if (!res.ok) {
                    throw new Error('Failed to fetch positions. Please try again.');
                }
                const data = await res.json();
                setPositions(data);
            } catch (err) {
                setError(err.message);
            } finally {
                setIsLoading(false);
            }
        };

        fetchPositions();
    }, []);

    const handleLogout = async () => {
        await fetch('/api/logout');
        router.push('/login');
    };

    const renderPositions = () => {
        if (isLoading) return <p>Loading positions...</p>;
        if (error) return <p className="error-message">{error}</p>;
        if (positions.length === 0) return <p>No open positions.</p>;

        return (
            <table className="positions-table">
                <thead>
                    <tr>
                        <th>Symbol</th>
                        <th>Net Quantity</th>
                        <th>MTM</th>
                    </tr>
                </thead>
                <tbody>
                    {positions.map((pos, index) => (
                        <tr key={index}>
                            <td>{pos.tradeSymbol}</td>
                            <td>{pos.netQty}</td>
                            <td className={pos.mtm >= 0 ? 'profit' : 'loss'}>{pos.mtm.toFixed(2)}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        );
    };

    return (
        <div className="dashboard-container">
            <h1>Welcome!</h1>
            <p>Your session token is stored securely in an httpOnly cookie.</p>
            <button onClick={handleLogout} className="logout-button">
                Logout
            </button>

            <div className="positions-section">
                <h2>Net Positions</h2>
                {renderPositions()}
            </div>
        </div>
    );
}