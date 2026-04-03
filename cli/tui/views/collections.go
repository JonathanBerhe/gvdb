package views

import (
	"fmt"
	"strings"

	"gvdb-cli/client"
	pb "gvdb-cli/pb"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var (
	colHeaderStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#555555")).Bold(true)
	colCellStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("#ededed"))
	colDimStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#888888"))
	colSelStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#ededed")).Bold(true).Background(lipgloss.Color("#1a1a1a"))
	colErrStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#ee0000"))
)

type collectionsMsg struct {
	collections []*pb.CollectionInfo
	err         error
}

type CollectionsModel struct {
	client      *client.Client
	collections []*pb.CollectionInfo
	cursor      int
	width       int
	height      int
	err         string
	loading     bool
}

func NewCollectionsModel(c *client.Client) CollectionsModel {
	return CollectionsModel{client: c}
}

func (m CollectionsModel) Init() tea.Cmd {
	return m.fetch()
}

func (m CollectionsModel) fetch() tea.Cmd {
	c := m.client
	return func() tea.Msg {
		resp, err := c.Service.ListCollections(c.Ctx(), &pb.ListCollectionsRequest{})
		if err != nil {
			return collectionsMsg{err: err}
		}
		return collectionsMsg{collections: resp.Collections}
	}
}

func (m CollectionsModel) Update(msg tea.Msg) (CollectionsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case collectionsMsg:
		m.loading = false
		if msg.err != nil {
			m.err = client.FormatError(msg.err)
			return m, nil
		}
		m.err = ""
		m.collections = msg.collections
		if m.cursor >= len(m.collections) {
			m.cursor = max(0, len(m.collections)-1)
		}
		return m, nil

	case tea.KeyMsg:
		switch {
		case key.Matches(msg, key.NewBinding(key.WithKeys("j", "down"))):
			if m.cursor < len(m.collections)-1 {
				m.cursor++
			}
		case key.Matches(msg, key.NewBinding(key.WithKeys("k", "up"))):
			if m.cursor > 0 {
				m.cursor--
			}
		case key.Matches(msg, key.NewBinding(key.WithKeys("r"))):
			m.loading = true
			return m, m.fetch()
		}
	}
	return m, nil
}

func (m CollectionsModel) View() string {
	var b strings.Builder

	if m.err != "" {
		b.WriteString(colErrStyle.Render("Error: " + m.err))
		b.WriteString("\n")
		return b.String()
	}

	if m.loading {
		b.WriteString(colDimStyle.Render("Loading..."))
		return b.String()
	}

	if len(m.collections) == 0 {
		b.WriteString(colDimStyle.Render("No collections. Press 'c' to create one."))
		return b.String()
	}

	// Header
	hdr := fmt.Sprintf("  %-24s %-6s %-6s %-12s %s",
		"NAME", "ID", "DIM", "METRIC", "VECTORS")
	b.WriteString(colHeaderStyle.Render(hdr))
	b.WriteString("\n")

	// Rows
	for i, c := range m.collections {
		row := fmt.Sprintf("  %-24s %-6d %-6d %-12s %d",
			truncate(c.CollectionName, 24),
			c.CollectionId,
			c.Dimension,
			c.MetricType,
			c.VectorCount,
		)
		if i == m.cursor {
			b.WriteString(colSelStyle.Render(row))
		} else {
			b.WriteString(colCellStyle.Render(row))
		}
		b.WriteString("\n")
	}

	b.WriteString("\n")
	b.WriteString(colDimStyle.Render(fmt.Sprintf("  %d collection(s)  │  Enter=browse vectors  r=refresh  c=create  d=drop", len(m.collections))))

	return b.String()
}

func (m CollectionsModel) SetSize(w, h int) CollectionsModel {
	m.width = w
	m.height = h
	return m
}

func (m CollectionsModel) SelectedCollection() string {
	if m.cursor < len(m.collections) {
		return m.collections[m.cursor].CollectionName
	}
	return ""
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
