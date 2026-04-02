import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import Layout from './components/layout/Layout'
import RadarView from './views/RadarView'
import VolDeskView from './views/VolDeskView'
import ResultsView from './views/ResultsView'
import VIXView from './views/VIXView'
import EarningsView from './views/EarningsView'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/radar" replace />} />
          <Route path="radar"    element={<RadarView />} />
          <Route path="vol-desk" element={<VolDeskView />} />
          <Route path="results"  element={<ResultsView />} />
          <Route path="vix"      element={<VIXView />} />
          <Route path="earnings" element={<EarningsView />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
