Feature: Multi-hop delivery through an intermediate device

  # Three actors in a line: Carol -> Bob -> Alice. Carol never dials Alice and
  # Alice never dials Carol, so the only way Carol's message can reach Alice
  # is by being forwarded through Bob. The golden path can't distinguish
  # "direct delivery works" from "forwarding works"; this scenario can.
  #
  # Carol is the SENDER on purpose. Both links were dialed "towards" Alice
  # (Carol dialed Bob, Bob dialed Alice), so from the receiving end each hop
  # is an ACCEPTED connection: Bob receives from Carol on a connection Carol
  # opened, and Alice receives from Bob on a connection Bob opened. The golden
  # path only ever sends from the dialer, so this is also the first scenario
  # where the accept side has to forward, not just receive.
  Scenario: A message crosses two hops to a device with no direct connection to the sender
    Given Alice is a device in the space, online
    And Bob is a device in the space, online and connected to Alice
    And Carol is a device in the space, online and connected to Bob
    And Alice and Bob and Carol share membership in the space
    When Carol sends the message "relayed via bob"
    Then Carol's conversation view shows "relayed via bob" within 5 seconds
    # Bob before Alice, so a failure says which hop broke.
    And Bob's conversation view shows "relayed via bob" within 10 seconds
    And Alice's conversation view shows "relayed via bob" within 10 seconds
