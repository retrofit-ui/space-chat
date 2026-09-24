Feature: Catching up after being offline

  # Bob is the one who DIALS Alice, and Bob is the one who gets killed. That
  # pairing is forced by a documented limitation (`world.rs`, relaunch_actor):
  # a relaunched actor comes back with a fresh TransportIdentity, so anyone
  # who dialed its OLD id can never reconnect to it. Killing Alice instead
  # would strand Bob's dial forever and the scenario would fail for a reason
  # that has nothing to do with catch-up. Bob re-dials Alice on relaunch
  # (relaunch_actor reuses his dial targets), Alice's id is unchanged, so the
  # link comes back.
  #
  # The send happens strictly BETWEEN the kill and the relaunch: there is no
  # live Bob to push to, so the only way "sent while bob was down" can appear
  # in his view afterwards is sync catch-up on reconnect. The kill step
  # asserts the process really is gone before the send, so this can't pass
  # via a push that landed on a still-dying Bob.
  Scenario: A message sent while a device was down is delivered once it comes back
    Given Alice is a device in the space, online
    And Bob is a device in the space, online and connected to Alice
    And Alice and Bob share membership in the space
    When Bob's app process is killed
    And Alice sends the message "sent while bob was down"
    # Alice's own render first, so a later failure at Bob is unambiguous.
    Then Alice's conversation view shows "sent while bob was down" within 5 seconds
    When Bob's app process is relaunched against the same data directory
    Then Bob's conversation view shows "sent while bob was down" within 10 seconds
