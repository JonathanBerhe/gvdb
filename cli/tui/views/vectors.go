package views

import (
	"encoding/json"
	"fmt"
	"strings"

	"gvdb-cli/client"
	pb "gvdb-cli/pb"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var (
	vecHeaderStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#555555")).Bold(true)
	vecCellStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("#ededed"))
	vecDimStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#888888"))
	vecSelStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#ededed")).Bold(true).Background(lipgloss.Color("#333333"))
	vecErrStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#ee0000"))
)

type vectorsMsg struct {
	vectors []*pb.VectorWithId
	total   uint64
	hasMore bool
	err     error
}

type VectorsModel struct {
	client     *client.Client
	collection string
	vectors    []*pb.VectorWithId
	total      uint64
	hasMore    bool
	offset     uint64
	limit      uint32
	cursor     int
	width      int
	height     int
	err        string
	loading    bool
}

func NewVectorsModel(c *client.Client) VectorsModel {
	return VectorsModel{client: c, limit: 20}
}

func (m VectorsModel) Init() tea.Cmd {
	if m.collection == "" {
		return nil
	}
	return m.fetch()
}

func (m VectorsModel) fetch() tea.Cmd {
	c := m.client
	col := m.collection
	limit := m.limit
	offset := m.offset
	return func() tea.Msg {
		resp, err := c.Service.ListVectors(c.Ctx(), &pb.ListVectorsRequest{
			CollectionName:  col,
			Limit:           limit,
			Offset:          offset,
			IncludeMetadata: true,
		})
		if err != nil {
			return vectorsMsg{err: err}
		}
		return vectorsMsg{
			vectors: resp.Vectors,
			total:   resp.TotalCount,
			hasMore: resp.HasMore,
		}
	}
}

func (m VectorsModel) Update(msg tea.Msg) (VectorsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case vectorsMsg:
		m.loading = false
		if msg.err != nil {
			m.err = client.FormatError(msg.err)
			return m, nil
		}
		m.err = ""
		m.vectors = msg.vectors
		m.total = msg.total
		m.hasMore = msg.hasMore
		m.cursor = 0
		return m, nil

	case tea.KeyMsg:
		switch {
		case key.Matches(msg, key.NewBinding(key.WithKeys("j", "down"))):
			if m.cursor < len(m.vectors)-1 {
				m.cursor++
			}
		case key.Matches(msg, key.NewBinding(key.WithKeys("k", "up"))):
			if m.cursor > 0 {
				m.cursor--
			}
		case key.Matches(msg, key.NewBinding(key.WithKeys("n", "right"))):
			if m.hasMore {
				m.offset += uint64(m.limit)
				m.loading = true
				return m, m.fetch()
			}
		case key.Matches(msg, key.NewBinding(key.WithKeys("p", "left"))):
			if m.offset > 0 {
				if m.offset > uint64(m.limit) {
					m.offset -= uint64(m.limit)
				} else {
					m.offset = 0
				}
				m.loading = true
				return m, m.fetch()
			}
		case key.Matches(msg, key.NewBinding(key.WithKeys("r"))):
			m.loading = true
			return m, m.fetch()
		}
	}
	return m, nil
}

func (m VectorsModel) View() string {
	var b strings.Builder

	if m.collection == "" {
		b.WriteString(vecDimStyle.Render("Select a collection from the Collections tab (press 1)"))
		return b.String()
	}

	b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#ededed")).Bold(true).Render(m.collection))
	b.WriteString(vecDimStyle.Render(fmt.Sprintf("  %d vectors", m.total)))
	b.WriteString("\n\n")

	if m.err != "" {
		b.WriteString(vecErrStyle.Render("Error: " + m.err))
		return b.String()
	}

	if m.loading {
		b.WriteString(vecDimStyle.Render("Loading..."))
		return b.String()
	}

	if len(m.vectors) == 0 {
		b.WriteString(vecDimStyle.Render("Empty collection."))
		return b.String()
	}

	// Header
	w := m.width - 4 // account for content padding
	if w < 80 {
		w = 80
	}
	metaWidth := w - 22 // 12 (ID) + 6 (DIM) + 4 (spacing)
	if metaWidth < 20 {
		metaWidth = 20
	}
	hdr := fmt.Sprintf("%-12s %-6s %s", "ID", "DIM", "METADATA")
	b.WriteString(vecHeaderStyle.Width(w).Render(hdr))
	b.WriteString("\n")

	// Rows
	for i, v := range m.vectors {
		mdStr := ""
		if v.Metadata != nil {
			md := make(map[string]any)
			for k, val := range v.Metadata.Fields {
				switch x := val.Value.(type) {
				case *pb.MetadataValue_IntValue:
					md[k] = x.IntValue
				case *pb.MetadataValue_DoubleValue:
					md[k] = x.DoubleValue
				case *pb.MetadataValue_StringValue:
					md[k] = x.StringValue
				case *pb.MetadataValue_BoolValue:
					md[k] = x.BoolValue
				}
			}
			raw, _ := json.Marshal(md)
			mdStr = string(raw)
			if len(mdStr) > metaWidth {
				mdStr = mdStr[:metaWidth-3] + "..."
			}
		}

		row := fmt.Sprintf("%-12d %-6d %s", v.Id, v.Vector.Dimension, mdStr)
		if i == m.cursor {
			b.WriteString(vecSelStyle.Width(w).Render(row))
		} else {
			b.WriteString(vecCellStyle.Width(w).Render(row))
		}
		b.WriteString("\n")
	}

	b.WriteString("\n")
	pageInfo := fmt.Sprintf("  %d–%d of %d", m.offset+1, m.offset+uint64(len(m.vectors)), m.total)
	b.WriteString(vecDimStyle.Render(pageInfo + "  │  n=next  p=prev  r=refresh"))

	return b.String()
}

func (m VectorsModel) SetSize(w, h int) VectorsModel {
	m.width = w
	m.height = h
	return m
}

func (m VectorsModel) SetCollection(name string) VectorsModel {
	m.collection = name
	m.offset = 0
	m.cursor = 0
	return m
}
