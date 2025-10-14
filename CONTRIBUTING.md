# Contributing to Nexus Backpressure

Thank you for your interest in contributing to Nexus Backpressure! This document provides guidelines and instructions for contributing.

**Project Creator**: [Tryboy869](https://github.com/Tryboy869) @ Nexus Studio 100  
**Contact**: nexusstudio100@gmail.com

## Code of Conduct

We are committed to providing a welcoming and inspiring community for all. Please be respectful of all contributors.

## Getting Started

### Prerequisites

- Go 1.21+
- Python 3.8+
- Node.js 18+
- Rust 1.70+
- Git

### Fork & Clone

```bash
git clone https://github.com/Tryboy869/nexus-backpressure.git
cd nexus-backpressure
```

## Development

### Reference Implementations

Each reference implementation is a **single file** with identical 9-section architecture:

1. Imports
2. Configuration
3. Security Gateway
4. Message Types
5. Core Engine
6. Orchestrator
7. HTTP API
8. CLI
9. Main Entry Point

### Adding a New Language

To add a new language implementation:

1. Create `reference-implementations/[language]/`
2. Create single-file implementation following 9-section architecture
3. Add configuration file for the ecosystem
4. Add `README.md` with setup instructions
5. Add tests in `tests/integration/`
6. Submit PR

Example structure:
```
reference-implementations/java/
├── Main.java               # Mono-file, 9 sections
├── pom.xml                 # Maven config
├── README.md               # Setup guide
└── .gitignore
```

### Modifying Existing Code

Keep the 9-section architecture intact. Changes should:

1. Maintain identical structure across all languages
2. Preserve backward compatibility
3. Include tests
4. Update documentation

## Testing

### Run Tests

```bash
# Go
cd reference-implementations/go && go test

# Python
cd reference-implementations/python && python -m pytest

# Node.js
cd reference-implementations/node && npm test

# Rust
cd reference-implementations/rust && cargo test
```

### Add Tests

- Unit tests in `tests/unit/`
- Integration tests in `tests/integration/`
- Performance tests in `tests/performance/`

## Documentation

### Updating Docs

- Protocol changes → Update `SPECIFICATION.md`
- Architecture changes → Update `docs/ARCHITECTURE.md`
- Integration examples → Add to `integrations/`
- New use cases → Add to `case-studies/`

### Writing Examples

Examples should be:
- Complete and working
- Well-commented
- Copy-paste ready
- Include README with explanation

## Submitting Changes

### Pull Request Process

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make your changes
4. Test thoroughly
5. Commit with clear messages: `git commit -m "feat: add xyz"`
6. Push: `git push origin feature/your-feature`
7. Open Pull Request with description

### PR Description Template

```markdown
## Description
Brief description of changes

## Type
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation
- [ ] Performance improvement

## Changes
- Change 1
- Change 2

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] No breaking changes

## Checklist
- [ ] Code follows style guidelines
- [ ] Documentation updated
- [ ] All tests passing
```

## Commit Message Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `test`: Tests
- `refactor`: Code refactoring
- `perf`: Performance improvement

Example:
```
feat(core): add rate limiting to security gateway

- Implement per-producer rate limiting
- Add configurable limits
- Add audit logging

Fixes #123
```

## Code Style

### Go
- Follow `gofmt`
- Use `golint`
- Run `go vet`

### Python
- Follow PEP 8
- Use `black` for formatting
- Use `mypy` for type checking

### Node.js
- Follow ESLint config
- Use `prettier` for formatting
- Support ES modules

### Rust
- Follow `rustfmt`
- Use `clippy` for linting
- Maintain memory safety

## Reporting Issues

### Bug Report

Include:
- Description of the issue
- Steps to reproduce
- Expected behavior
- Actual behavior
- Environment (OS, version, etc.)
- Logs or error messages

### Feature Request

Include:
- Clear description
- Use case / motivation
- Proposed solution
- Alternatives considered

## Communication

- **GitHub Issues**: Bug reports, feature requests
- **GitHub Discussions**: Questions, ideas
- **Email**: nexusstudio100@gmail.com
- **Twitter**: Follow for updates

## Recognition

Contributors will be:
- Listed in CONTRIBUTORS.md
- Credited in release notes
- Recognized in the community

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

---

**Questions?** Open a GitHub Discussion or email nexusstudio100@gmail.com