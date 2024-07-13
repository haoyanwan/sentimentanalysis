"use client";
// pages/index.tsx
import { useEffect, useState } from "react";
import Image from "next/image";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";

interface StockMention {
  time: string;
  density: number;
}

const Home: React.FC = () => {
  const [data, setData] = useState<StockMention[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // Fetch the stock mentions data from the API
    fetch("/api/stock/GME")
      .then((response) => response.json())
      .then((data: StockMention[]) => {
        if (Array.isArray(data)) {
          // Sort the data by time
          const sortedData = data.sort(
            (a, b) => new Date(a.time).getTime() - new Date(b.time).getTime()
          );
          console.log(sortedData); // For debugging
          setData(sortedData);
        } else {
          console.error("Unexpected data format:", data);
          setError("Unexpected data format received from API.");
        }
      })
      .catch((err) => {
        console.error("Error fetching data:", err);
        setError("Error fetching data.");
      });
  }, []);

  return (
    <div>
      <h1>Stock Mentions Density Over Time</h1>
      {error && <p style={{ color: "red" }}>{error}</p>}
      <ResponsiveContainer width="100%" height={400}>
        <AreaChart
          data={data}
          margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
        >
          <XAxis
            dataKey="time"
            type="category"
            domain={["dataMin", "dataMax"]}
            tickFormatter={(tick) => new Date(tick).toLocaleDateString()}
          />
          <YAxis scale="linear" domain={["auto", "auto"]} />
          <Tooltip
            labelFormatter={(label) => new Date(label).toLocaleDateString()}
          />
          <Legend />
          <Area
            type="monotone"
            dataKey="density"
            stroke="#8884d8"
            fillOpacity={0.7}
            fill="#8884d8"
          />
        </AreaChart>
      </ResponsiveContainer>
      <Image src="/test.jpg" alt="test" width={500} height={500} />
    </div>
  );
};

export default Home;
