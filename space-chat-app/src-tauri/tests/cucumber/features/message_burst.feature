Feature: A burst of messages all arrive

  # Membership seeding is deliberately SKIPPED here. The golden path seeds it,
  # which leaves open whether delivery actually depends on the receiver having
  # the sender in its local membership store. Today it doesn't: the app only
  # consults membership for `sender_name` when it builds the conversation spec
  # (`conversation_spec.rs`), never to decide whether to accept a message.
  # This scenario pins that: if some future change makes delivery
  # membership-gated, this fails while the golden path keeps passing.
  #
  # The four messages are asserted as a SET, not a sequence: cross-device
  # ordering of a rapid burst is documented as unspecified, and the step that
  # checks each one only cares that it is rendered somewhere in the view.
  Scenario: Four messages sent back to back all reach the other device
    Given Bob is a device in the space, online
    And Alice is a device in the space, online and connected to Bob
    When Alice sends the messages:
      | burst one   |
      | burst two   |
      | burst three |
      | burst four  |
    Then Bob's conversation view shows "burst one" within 10 seconds
    And Bob's conversation view shows "burst two" within 10 seconds
    And Bob's conversation view shows "burst three" within 10 seconds
    And Bob's conversation view shows "burst four" within 10 seconds
