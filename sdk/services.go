package sdk

// Service is a wire service the plugin adds, that is, one the emulator does not
// have of its own. Name is both the wire service (Request.Service,
// Route.Service) and the registry prefix its actions are keyed under.
type Service struct {
	Cloud string `json:"cloud"` // "aws" | "gcp"
	Name  string `json:"name"`  // [a-z0-9-]+, must not be a built-in
	Paths []Path `json:"paths"`
}

// Path maps one HTTP method and path pattern to an action. A pattern is a
// sequence of segments: a literal, {name} (one non-empty segment, captured and
// unescaped) or {name=prefix*} (one segment starting with prefix, captured
// whole), with an optional trailing :verb on the last segment.
type Path struct {
	Method  string `json:"method"` // GET POST PUT PATCH DELETE
	Pattern string `json:"pattern"`
	Action  string `json:"action"` // [A-Z][A-Za-z0-9]*
}

// CapServices is provided by a host that accepts Manifest.Services. A plugin
// that declares services lists it in Requires (Mux.Manifest adds it
// automatically), so an older host refuses the plugin clearly instead of
// mis-serving it.
const CapServices = "services"
