package tui

import "github.com/charmbracelet/lipgloss"

var (
	// Pitch black theme matching web UI
	colorBg        = lipgloss.Color("#000000")
	colorBgCard    = lipgloss.Color("#111111")
	colorBorder    = lipgloss.Color("#222222")
	colorPrimary   = lipgloss.Color("#ededed")
	colorSecondary = lipgloss.Color("#888888")
	colorTertiary  = lipgloss.Color("#555555")
	colorAccent    = lipgloss.Color("#0070f3")
	colorDanger    = lipgloss.Color("#ee0000")
	colorSuccess   = lipgloss.Color("#00cc88")

	styleApp = lipgloss.NewStyle().
			Background(colorBg)

	styleHeader = lipgloss.NewStyle().
			Foreground(colorPrimary).
			Bold(true).
			Padding(0, 1)

	styleTabActive = lipgloss.NewStyle().
			Foreground(colorPrimary).
			Bold(true).
			Padding(0, 2).
			Border(lipgloss.NormalBorder(), false, false, true, false).
			BorderForeground(colorPrimary)

	styleTabInactive = lipgloss.NewStyle().
				Foreground(colorTertiary).
				Padding(0, 2).
				Border(lipgloss.NormalBorder(), false, false, true, false).
				BorderForeground(colorBorder)

	styleStatusBar = lipgloss.NewStyle().
			Foreground(colorSecondary).
			Padding(0, 1)

	styleStatusConnected = lipgloss.NewStyle().
				Foreground(colorSuccess)

	styleTableHeader = lipgloss.NewStyle().
				Foreground(colorTertiary).
				Bold(true)

	styleTableCell = lipgloss.NewStyle().
			Foreground(colorPrimary)

	styleTableCellDim = lipgloss.NewStyle().
				Foreground(colorSecondary)

	styleSelectedRow = lipgloss.NewStyle().
				Foreground(colorPrimary).
				Bold(true).
				Background(lipgloss.Color("#1a1a1a"))

	styleTitle = lipgloss.NewStyle().
			Foreground(colorPrimary).
			Bold(true)

	styleHelp = lipgloss.NewStyle().
			Foreground(colorTertiary)

	styleError = lipgloss.NewStyle().
			Foreground(colorDanger)

	styleAccent = lipgloss.NewStyle().
			Foreground(colorAccent)
)
