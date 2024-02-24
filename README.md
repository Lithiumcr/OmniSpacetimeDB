> Project repository for ID2203 VT24 P3: OmniPaxos x SpaceTimeDB

# Omni-Paxos Spacetime DB - Rust Implementation

## Team Members (Alphabetical order by last name)
1. **Changrong Li**
2. **Duc Minh Pham**
3. **Ziyi Wang**

## Project Overview
This repository contains the implementation of a distributed database intended for a MMO game (Bitcraft), a project developed as part of the advanced distributed systems course. It is built upon the foundation provided by the [SpacetimeDB](https://github.com/clockworklabs/SpacetimeDB).

## Directory Structure
- **src/:** Contains the source code for the Omni-Paxos Spacetime DB implementation.
- **tests/:** Unit tests and integration tests for the implemented components.
- **figures/** Figures for visualisation.

```lua
OmniSpacetimeDB
├── src
│   ├── datastore
│   │   ├── example_datastore.rs
│   │   ├── mod.rs
│   │   └── ...
│   ├── durability
│   │   ├── example_durability.rs
│   │   ├── omnipaxos_durability.rs
│   │   ├── mod.rs
│   │   └── ...
│   ├── node
│   │   ├── mod.rs
│   │   └── ...
│   └── main.rs
├── tests
│   ├── datastore_tests.rs
│   ├── durability_tests.rs
│   └── node_tests.rs
├── figures
│   ├── figure1.webp
│   ├── figure2.webp
│   └── figure3.webp
├── Cargo.toml
├── README.md
├── .gitignore
└── ...
```

## Getting Started
To get started with the project, follow these steps:

1. Clone the repository:
   ```bash
   git clone https://github.com/Lithiumcr/OmniSpacetimeDB.git
   ```

2. Build the project:
   ```bash
   cd OmniPaxos-Spacetime-DB-Rust
   cargo build
   ```

3. Run tests:
   ```bash
   cargo test
   ```

## Contribution Guidelines

1. Fork the repository.
2. Switch to develop branch: `git checkout develop`.
3. Create a new branch for your feature/bugfix: `git checkout -b feature-name`.
4. Commit your changes: `git commit -m 'Add a descriptive commit message'`.
5. Push to the branch: `git push origin feature-name`.
6. Create a pull request with detailed information on your changes and merge to develop.
7. After testing on develop, create a pull request to merge to main.

## Contact
For any inquiries or discussions related to the project, please feel free to contact the team members.

## Credits
This project is based on the [SpacetimeDB](https://github.com/clockworklabs/SpacetimeDB) project by Clockwork Labs, licensed under GPLv3. We appreciate their contribution to the open-source community.
