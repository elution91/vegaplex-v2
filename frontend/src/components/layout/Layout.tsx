import { Outlet } from 'react-router-dom'
import Navbar from './Navbar'
import { ErrorBoundary } from '../shared/ErrorBoundary'

export default function Layout() {
  return (
    <div className="flex flex-col h-screen overflow-hidden">
      <Navbar />
<main className="flex-1 overflow-auto p-4">
        <ErrorBoundary>
          <Outlet />
        </ErrorBoundary>
      </main>
    </div>
  )
}
