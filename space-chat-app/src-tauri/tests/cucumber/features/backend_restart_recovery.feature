Feature: Backend restart recovery

  # Single-actor on purpose: nothing here is about sync, and a multi-actor
  # restart would run straight into Task 17's documented obstacle (a relaunched
  # actor comes back with a fresh random TransportIdentity, so a peer that was
  # dialing its OLD endpoint id cannot reconnect). Nothing needs to reconnect
  # TO Alice here -- she is proving that her OWN history survives her OWN
  # restart, which is the app-shell spec's "kill and restart the core
  # mid-session, verify the frontend recovers via full resend without a manual
  # reload" requirement. `space-chat-core` runs embedded in the same OS process
  # as the app shell, so "restart the core" is a kill/relaunch of the whole
  # `space-chat-app` process against the same on-disk data directory.
  Scenario: The app recovers full conversation history after a mid-session restart
    Given Alice is a device in the space, online
    And Alice has sent the message "before restart"
    # Asserted BEFORE the restart too (a `Then` because the step it reuses is
    # registered as one, and an `And` here would inherit `Given` and not match),
    # so a failure after the restart can never be confused with the message
    # having failed to render/persist in the first place.
    Then Alice's conversation view shows "before restart" within 5 seconds
    When Alice's app process is killed and relaunched against the same data directory
    Then Alice's conversation view shows "before restart" within 5 seconds
