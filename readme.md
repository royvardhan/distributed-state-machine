# Distributed State Machine

A Rust implementation of a distributed state machine using a consensus protocol for state synchronization across nodes. The system uses a proposal-acknowledgment-commit pattern to ensure consistent state transitions across the network.

## Features

- Distributed consensus protocol
- State transition validation
- Retry mechanism for message delivery
- Metrics collection
- Asynchronous communication using Tokio
- Comprehensive error handling
- Test coverage including unit and integration tests

## Architecture

The system consists of the following key components:

- **Node**: Main component that handles peer communication and state management
- **State Machine**: Supports three states (Init, Running, Stopped) with validated transitions
- **Message Types**:
  - Proposal: Initiates state change
  - Acknowledgment: Confirms receipt of proposal
  - Commit: Finalizes state change
  - Reject: Declines invalid state transitions

## Implementation Details

### State Transitions
The system enforces strict state transition rules:
- Init → Running
- Running → Stopped
- Stopped → Init

Any other state transitions are considered invalid and will be rejected.

### Message Flow
1. A node initiates a state change by broadcasting a proposal
2. Peer nodes validate the proposal and respond with acknowledgments
3. Once majority consensus is reached, the proposing node broadcasts a commit
4. All nodes apply the state change upon receiving the commit message

### Error Handling
The system includes comprehensive error handling for:
- Network failures
- Serialization errors
- Consensus timeouts
- Message delivery failures

### Retry Mechanism
- Configurable retry attempts for message delivery
- Adjustable delay between retry attempts
- Metrics tracking for successful/failed message delivery

### Configuration
The system supports configurable parameters including:
- Consensus timeout duration
- Message retry attempts
- Message retry delay intervals

### Metrics and Monitoring
Built-in metrics collection for:
- Message success/failure rates
- Node count
- Message processing statistics

## Testing

The implementation includes extensive testing:
- Unit tests for state transitions
- Message handling tests
- Integration tests for node communication
- Mock network implementations for testing
- Timeout and error condition testing

## Technical Requirements

- Rust (latest stable version)
- Tokio for async runtime
- Serde for serialization
- Metrics for monitoring
- Tracing for logging

## Network Communication

The system uses TCP for reliable communication between nodes:
- Asynchronous message handling
- JSON serialization for messages
- Built-in connection management
- Automatic reconnection handling

## Future Enhancements

- Message authentication (signature verification)
- Dynamic peer discovery
- State persistence
- Enhanced security features
- Performance optimizations
