Feature: Golden path message delivery

  Scenario: A message sent by one online device appears in another's conversation view
    Given Bob is a device in the space, online
    And Alice is a device in the space, online and connected to Bob
    And Alice and Bob share membership in the space
    When Alice sends the message "hello from alice"
    # Alice's own view first, deliberately: it splits the two halves of the
    # golden path apart, so a future failure says immediately whether the
    # local send/render path broke or only cross-device sync did.
    Then Alice's conversation view shows "hello from alice" within 5 seconds
    And Bob's conversation view shows "hello from alice" within 5 seconds
