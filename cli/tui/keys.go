package tui

import "github.com/charmbracelet/bubbles/key"

type keyMap struct {
	Quit       key.Binding
	Help       key.Binding
	Tab1       key.Binding
	Tab2       key.Binding
	Tab3       key.Binding
	Tab4       key.Binding
	TabNext    key.Binding
	TabPrev    key.Binding
	Enter      key.Binding
	Back       key.Binding
	Refresh    key.Binding
	Create     key.Binding
	Delete     key.Binding
	Up         key.Binding
	Down       key.Binding
	NextPage   key.Binding
	PrevPage   key.Binding
	Filter     key.Binding
}

var keys = keyMap{
	Quit:     key.NewBinding(key.WithKeys("q", "ctrl+c"), key.WithHelp("q", "quit")),
	Help:     key.NewBinding(key.WithKeys("?"), key.WithHelp("?", "help")),
	Tab1:     key.NewBinding(key.WithKeys("1"), key.WithHelp("1", "collections")),
	Tab2:     key.NewBinding(key.WithKeys("2"), key.WithHelp("2", "vectors")),
	Tab3:     key.NewBinding(key.WithKeys("3"), key.WithHelp("3", "search")),
	Tab4:     key.NewBinding(key.WithKeys("4"), key.WithHelp("4", "metrics")),
	TabNext:  key.NewBinding(key.WithKeys("tab"), key.WithHelp("tab", "next tab")),
	TabPrev:  key.NewBinding(key.WithKeys("shift+tab"), key.WithHelp("shift+tab", "prev tab")),
	Enter:    key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "select")),
	Back:     key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "back")),
	Refresh:  key.NewBinding(key.WithKeys("r"), key.WithHelp("r", "refresh")),
	Create:   key.NewBinding(key.WithKeys("c"), key.WithHelp("c", "create")),
	Delete:   key.NewBinding(key.WithKeys("d"), key.WithHelp("d", "delete")),
	Up:       key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("up/k", "up")),
	Down:     key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("down/j", "down")),
	NextPage: key.NewBinding(key.WithKeys("n", "right"), key.WithHelp("n", "next page")),
	PrevPage: key.NewBinding(key.WithKeys("p", "left"), key.WithHelp("p", "prev page")),
	Filter:   key.NewBinding(key.WithKeys("/"), key.WithHelp("/", "filter")),
}
