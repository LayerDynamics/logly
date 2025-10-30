#!/bin/bash
# versioning.sh - Version management utilities for Logly
# Handles version bumping, validation, and synchronization across project files

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Project files that contain version information
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

# Function to get current version from pyproject.toml
get_current_version() {
    if [ ! -f "$PYPROJECT_FILE" ]; then
        print_error "pyproject.toml not found"
        return 1
    fi

    grep '^version = ' "$PYPROJECT_FILE" | sed 's/version = "\(.*\)"/\1/'
}

# Function to validate version format
# Accepts: X.Y.Z or X.Y.Z-alpha.N, X.Y.Z-beta.N, X.Y.Z-rc.N
validate_version() {
    local version="$1"

    # Remove 'v' prefix if present
    version="${version#v}"

    if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-z]+(\.[0-9]+)?)?$ ]]; then
        print_error "Invalid version format: $version"
        print_info "Expected format: X.Y.Z or X.Y.Z-alpha.N, X.Y.Z-beta.N, X.Y.Z-rc.N"
        return 1
    fi

    return 0
}

# Function to parse version components
parse_version() {
    local version="$1"
    version="${version#v}"  # Remove 'v' prefix if present

    # Split version into base and prerelease
    if [[ "$version" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)(-(.+))?$ ]]; then
        MAJOR="${BASH_REMATCH[1]}"
        MINOR="${BASH_REMATCH[2]}"
        PATCH="${BASH_REMATCH[3]}"
        PRERELEASE="${BASH_REMATCH[5]}"
    else
        return 1
    fi
}

# Function to compare versions
# Returns: 0 if v1 == v2, 1 if v1 > v2, 2 if v1 < v2
compare_versions() {
    local v1="$1"
    local v2="$2"

    # Remove 'v' prefix
    v1="${v1#v}"
    v2="${v2#v}"

    if [ "$v1" = "$v2" ]; then
        return 0
    fi

    # Simple string comparison (works for semantic versioning)
    if [[ "$v1" > "$v2" ]]; then
        return 1
    else
        return 2
    fi
}

# Function to update version in pyproject.toml
update_pyproject_version() {
    local new_version="$1"
    new_version="${new_version#v}"  # Remove 'v' prefix

    if [ ! -f "$PYPROJECT_FILE" ]; then
        print_error "pyproject.toml not found"
        return 1
    fi

    # Create backup
    cp "$PYPROJECT_FILE" "${PYPROJECT_FILE}.bak"

    # Update version using sed
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        sed -i '' "s/^version = \".*\"/version = \"$new_version\"/" "$PYPROJECT_FILE"
    else
        # Linux
        sed -i "s/^version = \".*\"/version = \"$new_version\"/" "$PYPROJECT_FILE"
    fi

    print_success "Updated pyproject.toml to version $new_version"
}

# Function to update CHANGELOG.md
update_changelog() {
    local new_version="$1"
    local date="$2"

    new_version="${new_version#v}"  # Remove 'v' prefix

    if [ -z "$date" ]; then
        date=$(date +%Y-%m-%d)
    fi

    if [ ! -f "$CHANGELOG_FILE" ]; then
        print_warning "CHANGELOG.md not found, skipping changelog update"
        return 0
    fi

    # Check if version already exists in changelog
    if grep -q "## \[$new_version\]" "$CHANGELOG_FILE"; then
        print_warning "Version $new_version already exists in CHANGELOG.md"
        return 0
    fi

    # Create backup
    cp "$CHANGELOG_FILE" "${CHANGELOG_FILE}.bak"

    # Replace [Unreleased] with new version
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        sed -i '' "s/## \[Unreleased\]/## [Unreleased]\n\n## [$new_version] - $date/" "$CHANGELOG_FILE"
    else
        # Linux
        sed -i "s/## \[Unreleased\]/## [Unreleased]\n\n## [$new_version] - $date/" "$CHANGELOG_FILE"
    fi

    print_success "Updated CHANGELOG.md with version $new_version"
}

# Function to bump version
bump_version() {
    local bump_type="$1"  # major, minor, patch, prerelease
    local prerelease_type="$2"  # alpha, beta, rc (optional)

    local current_version
    current_version=$(get_current_version)

    if [ -z "$current_version" ]; then
        print_error "Could not determine current version"
        return 1
    fi

    print_info "Current version: $current_version"

    # Parse current version
    if ! parse_version "$current_version"; then
        print_error "Failed to parse current version"
        return 1
    fi

    local new_version=""

    case "$bump_type" in
        major)
            MAJOR=$((MAJOR + 1))
            MINOR=0
            PATCH=0
            new_version="${MAJOR}.${MINOR}.${PATCH}"
            ;;
        minor)
            MINOR=$((MINOR + 1))
            PATCH=0
            new_version="${MAJOR}.${MINOR}.${PATCH}"
            ;;
        patch)
            PATCH=$((PATCH + 1))
            new_version="${MAJOR}.${MINOR}.${PATCH}"
            ;;
        prerelease)
            if [ -z "$prerelease_type" ]; then
                print_error "Prerelease type required (alpha, beta, rc)"
                return 1
            fi

            if [ -n "$PRERELEASE" ]; then
                # Increment existing prerelease
                if [[ "$PRERELEASE" =~ ^([a-z]+)\.([0-9]+)$ ]]; then
                    local pre_type="${BASH_REMATCH[1]}"
                    local pre_num="${BASH_REMATCH[2]}"
                    pre_num=$((pre_num + 1))
                    new_version="${MAJOR}.${MINOR}.${PATCH}-${pre_type}.${pre_num}"
                else
                    new_version="${MAJOR}.${MINOR}.${PATCH}-${PRERELEASE}.1"
                fi
            else
                # Create new prerelease
                new_version="${MAJOR}.${MINOR}.${PATCH}-${prerelease_type}.1"
            fi
            ;;
        *)
            print_error "Invalid bump type: $bump_type"
            print_info "Valid types: major, minor, patch, prerelease"
            return 1
            ;;
    esac

    print_info "New version: $new_version"

    # Update files
    update_pyproject_version "$new_version"

    # Don't update changelog for prereleases
    if [[ "$new_version" != *"-"* ]]; then
        update_changelog "$new_version"
    fi

    print_success "Version bumped from $current_version to $new_version"
    echo "$new_version"
}

# Function to set a specific version
set_version() {
    local new_version="$1"
    local skip_changelog="$2"

    new_version="${new_version#v}"  # Remove 'v' prefix

    if ! validate_version "$new_version"; then
        return 1
    fi

    local current_version
    current_version=$(get_current_version)

    print_info "Current version: $current_version"
    print_info "Setting version to: $new_version"

    # Update files
    update_pyproject_version "$new_version"

    if [ "$skip_changelog" != "--skip-changelog" ]; then
        update_changelog "$new_version"
    fi

    print_success "Version set to $new_version"
}

# Function to check if version is consistent across files
check_version_consistency() {
    print_info "Checking version consistency..."

    local pyproject_version
    pyproject_version=$(get_current_version)

    print_info "pyproject.toml: $pyproject_version"

    # Check if there are uncommitted changes
    if command -v git &> /dev/null; then
        if ! git diff --quiet HEAD -- "$PYPROJECT_FILE" 2>/dev/null; then
            print_warning "pyproject.toml has uncommitted changes"
        fi
    fi

    print_success "Version consistency check complete"
}

# Function to show current version
show_version() {
    local version
    version=$(get_current_version)
    echo "$version"
}

# Function to create a version tag
create_version_tag() {
    local version="$1"
    local message="$2"

    version="${version#v}"  # Remove 'v' prefix

    if ! validate_version "$version"; then
        return 1
    fi

    if ! command -v git &> /dev/null; then
        print_error "git is not installed"
        return 1
    fi

    # Check if we're in a git repository
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        print_error "Not in a git repository"
        return 1
    fi

    local tag="v${version}"

    # Check if tag already exists
    if git rev-parse "$tag" >/dev/null 2>&1; then
        print_error "Tag $tag already exists"
        return 1
    fi

    if [ -z "$message" ]; then
        message="Release version $tag"
    fi

    # Create annotated tag
    git tag -a "$tag" -m "$message"
    print_success "Created git tag: $tag"
    print_info "Push tag with: git push origin $tag"
}

# Main script logic
main() {
    local command="$1"
    shift

    case "$command" in
        get|current|show)
            show_version
            ;;
        validate)
            if [ -z "$1" ]; then
                print_error "Version required"
                exit 1
            fi
            validate_version "$1"
            print_success "Version $1 is valid"
            ;;
        bump)
            if [ -z "$1" ]; then
                print_error "Bump type required (major, minor, patch, prerelease)"
                exit 1
            fi
            bump_version "$1" "$2"
            ;;
        set)
            if [ -z "$1" ]; then
                print_error "Version required"
                exit 1
            fi
            set_version "$1" "$2"
            ;;
        check)
            check_version_consistency
            ;;
        tag)
            local version="$1"
            if [ -z "$version" ]; then
                version=$(get_current_version)
            fi
            create_version_tag "$version" "$2"
            ;;
        *)
            cat << EOF
Usage: $0 <command> [options]

Commands:
  get|current|show              Show current version
  validate <version>            Validate version format
  bump <type> [prerelease-type] Bump version (major, minor, patch, prerelease)
  set <version> [--skip-changelog]  Set specific version
  check                         Check version consistency
  tag [version] [message]       Create git tag for version

Examples:
  $0 get                        # Show current version
  $0 bump minor                 # Bump minor version
  $0 bump prerelease alpha      # Create alpha prerelease
  $0 set 1.2.3                  # Set version to 1.2.3
  $0 tag                        # Tag current version
  $0 tag 1.2.3 "Release v1.2.3" # Tag specific version with message

Version Format:
  Stable:     X.Y.Z (e.g., 1.2.3)
  Prerelease: X.Y.Z-alpha.N, X.Y.Z-beta.N, X.Y.Z-rc.N
EOF
            exit 1
            ;;
    esac
}

# Run main if script is executed directly
if [ "${BASH_SOURCE[0]}" -ef "$0" ]; then
    main "$@"
fi
