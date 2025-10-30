#!/bin/bash
# release.sh - Automated release workflow for Logly
# Handles version bumping, changelog updates, git operations, and GitHub release creation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSIONING_SCRIPT="$SCRIPT_DIR/versioning.sh"

# Configuration
DEFAULT_BRANCH="main"
PYPROJECT_FILE="pyproject.toml"
CHANGELOG_FILE="CHANGELOG.md"

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "\n${CYAN}==>${NC} ${BOLD}$1${NC}\n"
}

# Function to check if required tools are installed
check_requirements() {
    local missing_tools=()

    if ! command -v git &> /dev/null; then
        missing_tools+=("git")
    fi

    if ! command -v gh &> /dev/null; then
        print_warning "GitHub CLI (gh) not found - GitHub release creation will be skipped"
        print_info "Install with: brew install gh (macOS) or https://cli.github.com"
    fi

    if [ ${#missing_tools[@]} -ne 0 ]; then
        print_error "Missing required tools: ${missing_tools[*]}"
        exit 1
    fi

    if [ ! -f "$VERSIONING_SCRIPT" ]; then
        print_error "versioning.sh not found at $VERSIONING_SCRIPT"
        exit 1
    fi
}

# Wrapper functions that call versioning.sh
get_current_version() {
    "$VERSIONING_SCRIPT" get
}

validate_version() {
    "$VERSIONING_SCRIPT" validate "$1" > /dev/null 2>&1
}

bump_version() {
    "$VERSIONING_SCRIPT" bump "$@"
}

create_version_tag() {
    "$VERSIONING_SCRIPT" tag "$@"
}

# Function to check git status
check_git_status() {
    print_step "Checking git repository status"

    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        print_error "Not in a git repository"
        exit 1
    fi

    # Check for uncommitted changes
    if ! git diff-index --quiet HEAD -- 2>/dev/null; then
        print_error "Working directory has uncommitted changes"
        print_info "Please commit or stash your changes before releasing"
        git status --short
        exit 1
    fi

    # Check current branch
    local current_branch
    current_branch=$(git rev-parse --abbrev-ref HEAD)
    print_info "Current branch: $current_branch"

    # Warn if not on default branch
    if [ "$current_branch" != "$DEFAULT_BRANCH" ]; then
        print_warning "Not on $DEFAULT_BRANCH branch"
        read -p "Continue anyway? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_info "Release cancelled"
            exit 0
        fi
    fi

    # Fetch latest changes
    print_info "Fetching latest changes..."
    git fetch origin

    # Check if local is behind remote
    local local_commit
    local remote_commit
    local_commit=$(git rev-parse @)
    remote_commit=$(git rev-parse @{u} 2>/dev/null || echo "")

    if [ -n "$remote_commit" ] && [ "$local_commit" != "$remote_commit" ]; then
        local base_commit
        base_commit=$(git merge-base @ @{u})

        if [ "$local_commit" = "$base_commit" ]; then
            print_error "Local branch is behind remote. Please pull latest changes."
            exit 1
        elif [ "$remote_commit" != "$base_commit" ]; then
            print_error "Local and remote branches have diverged"
            exit 1
        fi
    fi

    print_success "Git repository is clean and up-to-date"
}

# Function to run tests
run_tests() {
    print_step "Running tests"

    if [ ! -f "tests" ] && [ ! -d "tests" ]; then
        print_warning "No tests directory found, skipping tests"
        return 0
    fi

    # Check if pytest is available
    if ! command -v pytest &> /dev/null; then
        print_warning "pytest not found, skipping tests"
        return 0
    fi

    print_info "Running test suite..."
    if pytest tests/ -v --cov=logly; then
        print_success "All tests passed"
    else
        print_error "Tests failed"
        read -p "Continue with release anyway? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_info "Release cancelled"
            exit 1
        fi
    fi
}

# Function to build distribution packages
build_packages() {
    print_step "Building distribution packages"

    # Check if build is available
    if ! python -m build --help &> /dev/null; then
        print_warning "python build module not found"
        print_info "Installing build module..."
        pip install --upgrade build
    fi

    # Clean previous builds
    if [ -d "dist" ]; then
        print_info "Cleaning previous builds..."
        rm -rf dist/
    fi

    # Build packages
    print_info "Building wheel and source distribution..."
    python -m build

    if [ $? -eq 0 ]; then
        print_success "Packages built successfully"
        ls -lh dist/
    else
        print_error "Package build failed"
        exit 1
    fi
}

# Function to create git commit and tag
create_git_tag() {
    local version="$1"
    local skip_commit="$2"

    print_step "Creating git commit and tag"

    # Add changed files
    git add "$PYPROJECT_FILE"

    if [ -f "$CHANGELOG_FILE" ]; then
        git add "$CHANGELOG_FILE"
    fi

    # Create commit unless skipped
    if [ "$skip_commit" != "--skip-commit" ]; then
        local commit_message="Release version $version"
        print_info "Creating commit: $commit_message"
        git commit -m "$commit_message"
        print_success "Commit created"
    fi

    # Create tag using versioning script
    print_info "Creating tag v$version..."
    create_version_tag "$version" "Release version $version"
    print_success "Tag v$version created"
}

# Function to push changes to remote
push_to_remote() {
    local version="$1"
    local push_commit="$2"

    print_step "Pushing changes to remote"

    local current_branch
    current_branch=$(git rev-parse --abbrev-ref HEAD)

    # Push commit if requested
    if [ "$push_commit" = "true" ]; then
        print_info "Pushing commit to $current_branch..."
        git push origin "$current_branch"
        print_success "Commit pushed"
    fi

    # Push tag
    print_info "Pushing tag v$version..."
    git push origin "v$version"
    print_success "Tag v$version pushed"

    print_info "GitHub Actions will now build and create the release"
    print_info "Monitor progress at: https://github.com/$(git remote get-url origin | sed 's/.*github.com[:/]\(.*\)\.git/\1/')/actions"
}

# Function to create GitHub release (if gh is available)
create_github_release() {
    local version="$1"

    if ! command -v gh &> /dev/null; then
        print_info "GitHub CLI not available, skipping GitHub release creation"
        print_info "The release will be created automatically by GitHub Actions"
        return 0
    fi

    print_step "Creating GitHub release"

    # Check if user is authenticated
    if ! gh auth status &> /dev/null; then
        print_warning "Not authenticated with GitHub CLI"
        print_info "Run 'gh auth login' to authenticate"
        return 0
    fi

    # Determine if prerelease
    local prerelease_flag=""
    if [[ "$version" =~ (alpha|beta|rc) ]]; then
        prerelease_flag="--prerelease"
        print_info "Detected prerelease version"
    fi

    # Extract release notes from CHANGELOG
    local release_notes=""
    if [ -f "$CHANGELOG_FILE" ]; then
        print_info "Extracting release notes from CHANGELOG.md..."
        # This is a simple extraction - you might want to improve this
        release_notes=$(awk "/## \[$version\]/,/## \[/" "$CHANGELOG_FILE" | sed '1d;$d')
    fi

    if [ -z "$release_notes" ]; then
        release_notes="Release version $version"
    fi

    print_info "Creating GitHub release v$version..."
    if gh release create "v$version" \
        --title "Logly v$version" \
        --notes "$release_notes" \
        $prerelease_flag; then
        print_success "GitHub release created"
    else
        print_warning "Failed to create GitHub release (may be created by Actions)"
    fi
}

# Function to perform full release workflow
do_release() {
    local version_type="$1"
    local prerelease_type="$2"
    local skip_tests="$3"
    local skip_build="$4"
    local skip_push="$5"

    print_info "Starting release workflow..."
    print_info "Version type: $version_type"

    # Check requirements
    check_requirements

    # Check git status
    check_git_status

    # Run tests
    if [ "$skip_tests" != "--skip-tests" ]; then
        run_tests
    else
        print_warning "Skipping tests"
    fi

    # Get current version
    local current_version
    current_version=$(get_current_version)
    print_info "Current version: $current_version"

    # Bump version
    print_step "Bumping version"
    local new_version
    if [ "$version_type" = "prerelease" ]; then
        new_version=$(bump_version "$version_type" "$prerelease_type")
    else
        new_version=$(bump_version "$version_type")
    fi

    if [ -z "$new_version" ]; then
        print_error "Failed to bump version"
        exit 1
    fi

    new_version="${new_version#v}"  # Remove 'v' prefix if present
    print_success "Version bumped to $new_version"

    # Build packages
    if [ "$skip_build" != "--skip-build" ]; then
        build_packages
    else
        print_warning "Skipping package build"
    fi

    # Create git commit and tag
    create_git_tag "$new_version"

    # Push to remote
    if [ "$skip_push" != "--skip-push" ]; then
        read -p "Push changes to remote? (Y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Nn]$ ]]; then
            push_to_remote "$new_version" "true"
            # Don't create GitHub release here - let Actions handle it
            # create_github_release "$new_version"
        else
            print_info "Changes not pushed. Push manually with:"
            print_info "  git push origin $(git rev-parse --abbrev-ref HEAD)"
            print_info "  git push origin v$new_version"
        fi
    else
        print_warning "Skipping push to remote"
        print_info "Push manually with:"
        print_info "  git push origin $(git rev-parse --abbrev-ref HEAD)"
        print_info "  git push origin v$new_version"
    fi

    print_success "Release workflow complete!"
    print_info "New version: v$new_version"
}

# Function to perform quick release (version already set)
quick_release() {
    local version="$1"
    local skip_tests="$2"

    print_info "Starting quick release for version $version..."

    # Check requirements
    check_requirements

    # Validate version
    if ! validate_version "$version"; then
        exit 1
    fi

    # Check git status
    check_git_status

    # Run tests
    if [ "$skip_tests" != "--skip-tests" ]; then
        run_tests
    fi

    # Build packages
    build_packages

    # Create git commit and tag
    create_git_tag "$version" "--skip-commit"

    # Push to remote
    read -p "Push tag to remote? (Y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        push_to_remote "$version" "false"
    fi

    print_success "Quick release complete!"
}

# Function to show release status
show_status() {
    print_step "Release Status"

    # Get current version
    local version
    version=$(get_current_version)
    print_info "Current version: $version"

    # Check git tags
    if git rev-parse "v$version" >/dev/null 2>&1; then
        print_success "Tag v$version exists"
    else
        print_warning "Tag v$version does not exist"
    fi

    # Check if there are uncommitted changes
    if git diff-index --quiet HEAD -- 2>/dev/null; then
        print_success "Working directory is clean"
    else
        print_warning "Working directory has uncommitted changes"
    fi

    # Check GitHub release (if gh available)
    if command -v gh &> /dev/null && gh auth status &> /dev/null 2>&1; then
        print_info "Checking GitHub releases..."
        gh release list --limit 5
    fi
}

# Function to rollback last release
rollback() {
    print_warning "Rolling back last release..."

    local current_version
    current_version=$(get_current_version)

    read -p "Rollback version $current_version? This will delete the tag. (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Rollback cancelled"
        exit 0
    fi

    # Delete local tag
    if git rev-parse "v$current_version" >/dev/null 2>&1; then
        git tag -d "v$current_version"
        print_success "Deleted local tag v$current_version"
    fi

    # Delete remote tag
    read -p "Delete remote tag? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git push origin ":refs/tags/v$current_version"
        print_success "Deleted remote tag v$current_version"
    fi

    # Restore backup files if they exist
    if [ -f "${PYPROJECT_FILE}.bak" ]; then
        mv "${PYPROJECT_FILE}.bak" "$PYPROJECT_FILE"
        print_success "Restored $PYPROJECT_FILE"
    fi

    if [ -f "${CHANGELOG_FILE}.bak" ]; then
        mv "${CHANGELOG_FILE}.bak" "$CHANGELOG_FILE"
        print_success "Restored $CHANGELOG_FILE"
    fi

    print_success "Rollback complete"
}

# Main script logic
main() {
    local command="$1"
    shift

    case "$command" in
        major|minor|patch)
            do_release "$command" "" "$@"
            ;;
        prerelease|pre)
            local prerelease_type="$1"
            if [ -z "$prerelease_type" ]; then
                print_error "Prerelease type required (alpha, beta, rc)"
                exit 1
            fi
            shift
            do_release "prerelease" "$prerelease_type" "$@"
            ;;
        quick)
            local version="$1"
            if [ -z "$version" ]; then
                print_error "Version required for quick release"
                exit 1
            fi
            shift
            quick_release "$version" "$@"
            ;;
        status)
            show_status
            ;;
        rollback)
            rollback
            ;;
        *)
            cat << EOF
${CYAN}Logly Release Management Script${NC}

Usage: $0 <command> [options]

${YELLOW}Release Commands:${NC}
  major [--skip-tests] [--skip-build] [--skip-push]
                                Bump major version and release (X.0.0)
  minor [--skip-tests] [--skip-build] [--skip-push]
                                Bump minor version and release (0.X.0)
  patch [--skip-tests] [--skip-build] [--skip-push]
                                Bump patch version and release (0.0.X)
  prerelease <type> [options]   Create prerelease (alpha, beta, rc)
  quick <version> [--skip-tests]
                                Quick release with existing version

${YELLOW}Other Commands:${NC}
  status                        Show current release status
  rollback                      Rollback last release (delete tag)

${YELLOW}Options:${NC}
  --skip-tests                  Skip running tests
  --skip-build                  Skip building packages
  --skip-push                   Skip pushing to remote

${YELLOW}Examples:${NC}
  $0 patch                      # Release patch version (0.1.0 -> 0.1.1)
  $0 minor                      # Release minor version (0.1.1 -> 0.2.0)
  $0 major                      # Release major version (0.2.0 -> 1.0.0)
  $0 prerelease alpha           # Create alpha release (1.0.0-alpha.1)
  $0 prerelease beta            # Create beta release (1.0.0-beta.1)
  $0 prerelease rc              # Create release candidate (1.0.0-rc.1)
  $0 quick 1.2.3                # Quick release for version 1.2.3
  $0 status                     # Show release status
  $0 rollback                   # Rollback last release

${YELLOW}Release Workflow:${NC}
  1. Checks git repository status (must be clean)
  2. Runs test suite (unless --skip-tests)
  3. Bumps version in pyproject.toml
  4. Updates CHANGELOG.md (for stable releases)
  5. Builds distribution packages (unless --skip-build)
  6. Creates git commit and tag
  7. Pushes to remote (unless --skip-push)
  8. GitHub Actions automatically creates the release

${YELLOW}Requirements:${NC}
  - git (required)
  - gh (optional, for GitHub operations)
  - pytest (optional, for running tests)
  - python build module (for building packages)

${YELLOW}Notes:${NC}
  - Always commit your changes before releasing
  - Tags trigger GitHub Actions workflows
  - Prereleases don't update CHANGELOG.md
  - Use 'gh auth login' to enable GitHub CLI features
EOF
            exit 1
            ;;
    esac
}

# Run main if script is executed directly
if [ "${BASH_SOURCE[0]}" -ef "$0" ]; then
    main "$@"
fi
