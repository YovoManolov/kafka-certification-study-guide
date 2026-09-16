# Website Deployment Strategy

## Recommended architecture

Use **mdBook + Mermaid + GitHub Actions + GitHub Pages**.

```mermaid
flowchart LR
    A[Markdown chapters] --> B[Git repository]
    B --> C[GitHub Actions]
    C --> D[Install mdBook]
    D --> E[Install mdbook-mermaid]
    E --> F[mdbook build]
    F --> G[Static book directory]
    G --> H[GitHub Pages]
    H --> I[Public study website]
```

## Why this fits the project

### Markdown remains the source of truth

The chapters remain easy to edit, diff and review.

### mdBook provides the book experience

It gives the project:

- navigation
- sidebar
- previous/next chapter navigation
- client-side search
- code highlighting
- responsive rendering
- static HTML output

### Mermaid replaces large ASCII schemas

Architecture, data flow and diagnostic diagrams are maintained as text inside Markdown.

Example:

```mermaid
flowchart LR
    Producer --> Broker
    Broker --> Consumer
```

This makes diagrams easier to update and keeps the repository portable.

### GitHub Pages is the hosting layer

The generated `book/` directory is a static website. GitHub Pages hosts the generated artifact; no application server is required.

## Deployment flow

```mermaid
sequenceDiagram
    participant Dev as Developer
    participant Git as GitHub
    participant CI as GitHub Actions
    participant Pages as GitHub Pages

    Dev->>Git: Push to main
    Git->>CI: Start workflow
    CI->>CI: Install mdBook
    CI->>CI: Install Mermaid preprocessor
    CI->>CI: Build static book
    CI->>Pages: Deploy artifact
    Pages-->>Dev: Published website
```

## One-time GitHub configuration

In the repository:

**Settings → Pages → Build and deployment → Source → GitHub Actions**

The repository's workflow is already prepared for this model.

## Repository workflow

```text
main
 |
 +-- Markdown changes
 |
 v
GitHub Actions
 |
 +-- build
 |
 +-- Mermaid rendering
 |
 +-- upload Pages artifact
 |
 v
GitHub Pages
```

## Branch strategy

Recommended:

```text
main
 |
 +-- stable published book

feature/chapter-xx-...
 |
 +-- chapter/content changes

feature/website-...
 |
 +-- theme/deployment changes
```

Only `main` should publish the production study website.

Pull requests can be used for review before merging.

## Quality gate

Before merging a documentation change:

```mermaid
flowchart LR
    A[Edit Markdown] --> B[Check links]
    B --> C[Check Mermaid]
    C --> D[Build mdBook]
    D --> E{Build succeeds?}
    E -->|No| F[Fix]
    F --> D
    E -->|Yes| G[Merge]
    G --> H[GitHub Pages deployment]
```

## Version pinning

The workflow pins:

- mdBook
- mdbook-mermaid

This makes CI builds reproducible instead of silently changing when upstream releases a new version.

When upgrading either dependency:

1. test locally
2. update the version in the workflow
3. update `scripts/setup-docs.sh`
4. build the complete book
5. inspect Mermaid diagrams
6. merge

## Custom domain

If the project later gets a dedicated domain, GitHub Pages supports custom domains. The domain configuration should be added through the repository's Pages settings rather than relying only on a `CNAME` file.

## What should not be committed

Do not commit:

- generated `book/`
- local IDE metadata
- temporary build files
- personal credentials
- GitHub tokens
- cloud credentials

The repository should contain the Markdown source and reproducible build/deployment configuration.
