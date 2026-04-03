package views

import (
	"fmt"
	"strings"
	"time"

	"gvdb-cli/client"
	pb "gvdb-cli/pb"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var (
	metLabelStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#555555")).Bold(true)
	metValueStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#ededed")).Bold(true)
	metDimStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("#888888"))
	metAccStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("#0070f3")).Bold(true)
	metErrStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("#ee0000"))
	metCardStyle  = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#222222")).
			Padding(1, 2).
			Width(24)
)

type statsMsg struct {
	stats *pb.GetStatsResponse
	err   error
}

type tickMsg time.Time

type MetricsModel struct {
	client *client.Client
	stats  *pb.GetStatsResponse
	width  int
	height int
	err    string
}

func NewMetricsModel(c *client.Client) MetricsModel {
	return MetricsModel{client: c}
}

func (m MetricsModel) Init() tea.Cmd {
	return tea.Batch(m.fetchStats(), m.tick())
}

func (m MetricsModel) tick() tea.Cmd {
	return tea.Tick(5*time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m MetricsModel) fetchStats() tea.Cmd {
	c := m.client
	return func() tea.Msg {
		resp, err := c.Service.GetStats(c.Ctx(), &pb.GetStatsRequest{})
		if err != nil {
			return statsMsg{err: err}
		}
		return statsMsg{stats: resp}
	}
}

func (m MetricsModel) Update(msg tea.Msg) (MetricsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case statsMsg:
		if msg.err != nil {
			m.err = client.FormatError(msg.err)
			return m, nil
		}
		m.err = ""
		m.stats = msg.stats
		return m, nil

	case tickMsg:
		return m, tea.Batch(m.fetchStats(), m.tick())
	}
	return m, nil
}

func (m MetricsModel) View() string {
	var b strings.Builder

	if m.err != "" {
		b.WriteString(metErrStyle.Render("Error: " + m.err))
		b.WriteString("\n")
		return b.String()
	}

	if m.stats == nil {
		b.WriteString(metDimStyle.Render("Loading metrics..."))
		return b.String()
	}

	b.WriteString(metLabelStyle.Render("SERVER METRICS"))
	b.WriteString(metDimStyle.Render("  (auto-refresh 5s)"))
	b.WriteString("\n\n")

	// Stat cards
	cards := []string{
		metCardStyle.Render(
			metLabelStyle.Render("COLLECTIONS") + "\n" +
				metAccStyle.Render(fmt.Sprintf("%d", m.stats.TotalCollections)),
		),
		metCardStyle.Render(
			metLabelStyle.Render("VECTORS") + "\n" +
				metAccStyle.Render(formatCount(m.stats.TotalVectors)),
		),
		metCardStyle.Render(
			metLabelStyle.Render("QUERIES") + "\n" +
				metAccStyle.Render(formatCount(m.stats.TotalQueries)),
		),
		metCardStyle.Render(
			metLabelStyle.Render("AVG QUERY TIME") + "\n" +
				metAccStyle.Render(fmt.Sprintf("%.2fms", m.stats.AvgQueryTimeMs)),
		),
	}

	b.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, cards...))
	b.WriteString("\n\n")
	b.WriteString(metDimStyle.Render("  r=refresh"))

	return b.String()
}

func (m MetricsModel) SetSize(w, h int) MetricsModel {
	m.width = w
	m.height = h
	return m
}

func formatCount(n uint64) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	}
	return fmt.Sprintf("%d", n)
}
