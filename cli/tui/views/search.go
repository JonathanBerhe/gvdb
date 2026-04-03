package views

import (
	"encoding/json"
	"fmt"
	"strings"

	"gvdb-cli/client"
	pb "gvdb-cli/pb"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var (
	srcLabelStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("#555555")).Bold(true)
	srcResultStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#ededed"))
	srcDimStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#888888"))
	srcAccStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#0070f3"))
	srcErrStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#ee0000"))
)

type searchResultMsg struct {
	results     []*pb.SearchResultEntry
	queryTimeMs float32
	err         error
}

type SearchModel struct {
	client        *client.Client
	collectionInp textinput.Model
	vectorInp     textinput.Model
	textQueryInp  textinput.Model
	topKInp       textinput.Model
	results       []*pb.SearchResultEntry
	queryTimeMs   float32
	width         int
	height        int
	err           string
	loading       bool
	focusIdx      int
}

func NewSearchModel(c *client.Client) SearchModel {
	colInp := textinput.New()
	colInp.Placeholder = "collection_name"
	colInp.CharLimit = 64
	colInp.Focus()

	vecInp := textinput.New()
	vecInp.Placeholder = "[0.1, 0.2, 0.3, ...]"
	vecInp.CharLimit = 4096

	textInp := textinput.New()
	textInp.Placeholder = "text query for BM25 (optional)"
	textInp.CharLimit = 256

	topKInp := textinput.New()
	topKInp.Placeholder = "10"
	topKInp.CharLimit = 5

	return SearchModel{
		client:        c,
		collectionInp: colInp,
		vectorInp:     vecInp,
		textQueryInp:  textInp,
		topKInp:       topKInp,
	}
}

func (m SearchModel) Init() tea.Cmd {
	return textinput.Blink
}

func (m SearchModel) inputs() []*textinput.Model {
	return []*textinput.Model{&m.collectionInp, &m.vectorInp, &m.textQueryInp, &m.topKInp}
}

func (m SearchModel) Update(msg tea.Msg) (SearchModel, tea.Cmd) {
	switch msg := msg.(type) {
	case searchResultMsg:
		m.loading = false
		if msg.err != nil {
			m.err = client.FormatError(msg.err)
			return m, nil
		}
		m.err = ""
		m.results = msg.results
		m.queryTimeMs = msg.queryTimeMs
		return m, nil

	case tea.KeyMsg:
		switch {
		case key.Matches(msg, key.NewBinding(key.WithKeys("tab"))):
			inputs := m.inputs()
			inputs[m.focusIdx].Blur()
			m.focusIdx = (m.focusIdx + 1) % len(inputs)
			inputs[m.focusIdx].Focus()
			return m, nil
		case key.Matches(msg, key.NewBinding(key.WithKeys("enter"))):
			return m, m.runSearch()
		}
	}

	// Update focused input
	var cmd tea.Cmd
	switch m.focusIdx {
	case 0:
		m.collectionInp, cmd = m.collectionInp.Update(msg)
	case 1:
		m.vectorInp, cmd = m.vectorInp.Update(msg)
	case 2:
		m.textQueryInp, cmd = m.textQueryInp.Update(msg)
	case 3:
		m.topKInp, cmd = m.topKInp.Update(msg)
	}
	return m, cmd
}

func (m SearchModel) runSearch() tea.Cmd {
	c := m.client
	collection := m.collectionInp.Value()
	vectorStr := m.vectorInp.Value()
	textQuery := m.textQueryInp.Value()
	topKStr := m.topKInp.Value()

	return func() tea.Msg {
		if collection == "" {
			return searchResultMsg{err: fmt.Errorf("collection name required")}
		}

		topK := uint32(10)
		if topKStr != "" {
			var k int
			fmt.Sscanf(topKStr, "%d", &k)
			if k > 0 {
				topK = uint32(k)
			}
		}

		// Determine search mode
		hasVector := strings.TrimSpace(vectorStr) != ""
		hasText := strings.TrimSpace(textQuery) != ""

		if !hasVector && !hasText {
			return searchResultMsg{err: fmt.Errorf("provide a vector and/or text query")}
		}

		// Hybrid if both or text-only
		if hasText {
			req := &pb.HybridSearchRequest{
				CollectionName: collection,
				TextQuery:      textQuery,
				TopK:           topK,
				VectorWeight:   0.5,
				TextWeight:     0.5,
				TextField:      "text",
				ReturnMetadata: true,
			}
			if hasVector {
				var values []float32
				if err := json.Unmarshal([]byte(vectorStr), &values); err != nil {
					return searchResultMsg{err: fmt.Errorf("invalid vector JSON: %w", err)}
				}
				req.QueryVector = &pb.Vector{Values: values, Dimension: uint32(len(values))}
			} else {
				req.TextWeight = 1.0
				req.VectorWeight = 0.0
			}
			resp, err := c.Service.HybridSearch(c.Ctx(), req)
			if err != nil {
				return searchResultMsg{err: err}
			}
			return searchResultMsg{results: resp.Results, queryTimeMs: resp.QueryTimeMs}
		}

		// Vector-only search
		var values []float32
		if err := json.Unmarshal([]byte(vectorStr), &values); err != nil {
			return searchResultMsg{err: fmt.Errorf("invalid vector JSON: %w", err)}
		}
		resp, err := c.Service.Search(c.Ctx(), &pb.SearchRequest{
			CollectionName: collection,
			QueryVector:    &pb.Vector{Values: values, Dimension: uint32(len(values))},
			TopK:           topK,
			ReturnMetadata: true,
		})
		if err != nil {
			return searchResultMsg{err: err}
		}
		return searchResultMsg{results: resp.Results, queryTimeMs: resp.QueryTimeMs}
	}
}

func (m SearchModel) View() string {
	var b strings.Builder

	b.WriteString(srcLabelStyle.Render("COLLECTION"))
	b.WriteString("\n")
	b.WriteString(m.collectionInp.View())
	b.WriteString("\n\n")

	b.WriteString(srcLabelStyle.Render("QUERY VECTOR"))
	b.WriteString("\n")
	b.WriteString(m.vectorInp.View())
	b.WriteString("\n\n")

	b.WriteString(srcLabelStyle.Render("TEXT QUERY (HYBRID)"))
	b.WriteString("\n")
	b.WriteString(m.textQueryInp.View())
	b.WriteString("\n\n")

	b.WriteString(srcLabelStyle.Render("TOP K"))
	b.WriteString("\n")
	b.WriteString(m.topKInp.View())
	b.WriteString("\n\n")

	b.WriteString(srcDimStyle.Render("Tab=next field  Enter=search"))
	b.WriteString("\n\n")

	if m.err != "" {
		b.WriteString(srcErrStyle.Render("Error: " + m.err))
		b.WriteString("\n")
	}

	if m.loading {
		b.WriteString(srcDimStyle.Render("Searching..."))
		return b.String()
	}

	if len(m.results) > 0 {
		b.WriteString(srcDimStyle.Render(fmt.Sprintf("%d results in %.2fms", len(m.results), m.queryTimeMs)))
		b.WriteString("\n\n")

		hdr := fmt.Sprintf("  %-4s %-12s %-12s %s", "#", "ID", "DISTANCE", "METADATA")
		b.WriteString(srcLabelStyle.Render(hdr))
		b.WriteString("\n")

		for i, r := range m.results {
			mdStr := ""
			if r.Metadata != nil {
				md := make(map[string]any)
				for k, val := range r.Metadata.Fields {
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
				if len(mdStr) > 50 {
					mdStr = mdStr[:47] + "..."
				}
			}
			row := fmt.Sprintf("  %-4d %-12d %-12.6f %s", i+1, r.Id, r.Distance, mdStr)
			b.WriteString(srcResultStyle.Render(row))
			b.WriteString("\n")
		}
	}

	return b.String()
}

func (m SearchModel) SetSize(w, h int) SearchModel {
	m.width = w
	m.height = h
	m.collectionInp.Width = min(w-4, 60)
	m.vectorInp.Width = min(w-4, 80)
	m.textQueryInp.Width = min(w-4, 80)
	m.topKInp.Width = 10
	return m
}
