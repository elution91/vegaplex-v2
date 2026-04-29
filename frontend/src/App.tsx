import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import Layout from './components/layout/Layout'
import LoginView from './views/LoginView'
import RadarView from './views/RadarView'
import VolDeskView from './views/VolDeskView'
import ResultsView from './views/ResultsView'
import VIXView from './views/VixView'
import EarningsView from './views/EarningsView'
import ResourcesView from './views/ResourcesView'
import SettingsView from './views/SettingsView'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<LoginView />} />
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/radar" replace />} />
          <Route path="radar"     element={<RadarView />} />
          <Route path="vol-desk"  element={<VolDeskView />} />
          <Route path="results"   element={<ResultsView />} />
          <Route path="vix" element={<VIXView />} />
          <Route path="vix/status"    element={<VIXView />} />
          <Route path="vix/history"   element={<VIXView />} />
          <Route path="vix/analytics" element={<VIXView />} />
          <Route path="earnings"          element={<EarningsView />} />
          <Route path="earnings/earnings" element={<EarningsView />} />
          <Route path="earnings/macro"    element={<EarningsView />} />
          <Route path="resources" element={<ResourcesView />} />
          <Route path="settings"  element={<SettingsView />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
