package tui

import (
	"fmt"
	"strings"

	"gvdb-cli/client"
	"gvdb-cli/tui/views"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type activeView int

const (
	viewCollections activeView = iota
	viewVectors
	viewSearch
	viewMetrics
)

var tabNames = []string{"Collections", "Vectors", "Search", "Metrics"}

type App struct {
	client      *client.Client
	active      activeView
	collections views.CollectionsModel
	vectors     views.VectorsModel
	search      views.SearchModel
	metrics     views.MetricsModel
	width       int
	height      int
	err         string
	showHelp    bool
}

func NewApp(c *client.Client) App {
	return App{
		client:      c,
		active:      viewCollections,
		collections: views.NewCollectionsModel(c),
		vectors:     views.NewVectorsModel(c),
		search:      views.NewSearchModel(c),
		metrics:     views.NewMetricsModel(c),
	}
}

func (a App) Init() tea.Cmd {
	return tea.Batch(
		a.collections.Init(),
		a.metrics.Init(),
	)
}

func (a App) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		a.width = msg.Width
		a.height = msg.Height
		contentHeight := a.height - 4 // header + tabs + status
		a.collections = a.collections.SetSize(a.width, contentHeight)
		a.vectors = a.vectors.SetSize(a.width, contentHeight)
		a.search = a.search.SetSize(a.width, contentHeight)
		a.metrics = a.metrics.SetSize(a.width, contentHeight)
		return a, nil

	case tea.KeyMsg:
		// Global keys
		if a.showHelp {
			a.showHelp = false
			return a, nil
		}

		// When search view is active, only ctrl+c quits — other keys go to inputs
		if a.active == viewSearch {
			if msg.Type == tea.KeyCtrlC {
				return a, tea.Quit
			}
			if msg.Type == tea.KeyEsc {
				a.active = viewCollections
				return a, a.collections.Init()
			}
			// Number keys switch tabs even in search
			switch {
			case key.Matches(msg, keys.Tab1):
				a.active = viewCollections
				return a, a.collections.Init()
			case key.Matches(msg, keys.Tab2):
				a.active = viewVectors
				return a, nil
			case key.Matches(msg, keys.Tab4):
				a.active = viewMetrics
				return a, a.metrics.Init()
			}
			// Delegate everything else to search view
			var cmd tea.Cmd
			a.search, cmd = a.search.Update(msg)
			return a, cmd
		}

		switch {
		case key.Matches(msg, keys.Quit):
			return a, tea.Quit
		case key.Matches(msg, keys.Help):
			a.showHelp = !a.showHelp
			return a, nil
		case key.Matches(msg, keys.Tab1):
			a.active = viewCollections
			return a, a.collections.Init()
		case key.Matches(msg, keys.Tab2):
			a.active = viewVectors
			return a, nil
		case key.Matches(msg, keys.Tab3):
			a.active = viewSearch
			return a, a.search.Init()
		case key.Matches(msg, keys.Tab4):
			a.active = viewMetrics
			return a, a.metrics.Init()
		case key.Matches(msg, keys.TabNext):
			// Don't intercept Tab when search view is active — it cycles inputs
			if a.active != viewSearch {
				a.active = (a.active + 1) % 4
				return a, nil
			}
		case key.Matches(msg, keys.TabPrev):
			if a.active != viewSearch {
				a.active = (a.active + 3) % 4
				return a, nil
			}
		}

		// Handle enter on collections to drill into vectors
		if a.active == viewCollections && key.Matches(msg, keys.Enter) {
			if name := a.collections.SelectedCollection(); name != "" {
				a.active = viewVectors
				a.vectors = a.vectors.SetCollection(name)
				return a, a.vectors.Init()
			}
		}
	}

	// Delegate to active view
	var cmd tea.Cmd
	switch a.active {
	case viewCollections:
		a.collections, cmd = a.collections.Update(msg)
	case viewVectors:
		a.vectors, cmd = a.vectors.Update(msg)
	case viewSearch:
		a.search, cmd = a.search.Update(msg)
	case viewMetrics:
		a.metrics, cmd = a.metrics.Update(msg)
	}
	return a, cmd
}

func (a App) View() string {
	if a.width == 0 {
		return "Loading..."
	}

	// Header
	header := lipgloss.JoinHorizontal(lipgloss.Center,
		styleHeader.Render("GVDB"),
		styleHelp.Render("  "+a.client.Address()),
	)

	// Tabs
	var tabs []string
	for i, name := range tabNames {
		if activeView(i) == a.active {
			tabs = append(tabs, styleTabActive.Render(fmt.Sprintf("[%d] %s", i+1, name)))
		} else {
			tabs = append(tabs, styleTabInactive.Render(fmt.Sprintf("[%d] %s", i+1, name)))
		}
	}
	tabBar := lipgloss.JoinHorizontal(lipgloss.Top, tabs...)

	// Content
	var content string
	if a.showHelp {
		content = a.renderHelp()
	} else {
		switch a.active {
		case viewCollections:
			content = a.collections.View()
		case viewVectors:
			content = a.vectors.View()
		case viewSearch:
			content = a.search.View()
		case viewMetrics:
			content = a.metrics.View()
		}
	}

	// Status bar
	status := styleStatusConnected.Render("●") + " " +
		styleStatusBar.Render(a.client.Address()) +
		styleHelp.Render("  ?=help  q=quit")

	// Calculate content height: total - header(1) - tabbar(1) - statusbar(1) - borders(1)
	contentHeight := a.height - 4
	if contentHeight < 1 {
		contentHeight = 1
	}

	// Pad content to fill available height
	contentStyle := lipgloss.NewStyle().
		Width(a.width).
		Height(contentHeight).
		Padding(1, 2)

	statusStyle := lipgloss.NewStyle().
		Width(a.width)

	return lipgloss.JoinVertical(lipgloss.Left,
		header,
		tabBar,
		contentStyle.Render(content),
		statusStyle.Render(status),
	)
}

func (a App) renderHelp() string {
	help := []struct{ key, desc string }{
		{"1-4", "Switch tabs"},
		{"Tab/Shift+Tab", "Cycle tabs"},
		{"Enter", "Select / drill down"},
		{"Esc", "Back / close"},
		{"r", "Refresh"},
		{"c", "Create (collections)"},
		{"d", "Delete"},
		{"j/k", "Navigate up/down"},
		{"n/p", "Next/prev page"},
		{"/", "Filter"},
		{"q", "Quit"},
	}

	var b strings.Builder
	b.WriteString(styleTitle.Render("Keyboard Shortcuts"))
	b.WriteString("\n\n")
	for _, h := range help {
		b.WriteString(fmt.Sprintf("  %s  %s\n",
			styleAccent.Render(fmt.Sprintf("%-16s", h.key)),
			styleTableCell.Render(h.desc),
		))
	}
	return b.String()
}
