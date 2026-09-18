Feature: Attachment lazy-fetch placeholder transition

  # Single-actor on purpose: nothing here is about sync. What is under test is
  # Task 13's protocol handler answering a cache miss with a placeholder
  # instead of an error, and Task 16's AttachmentImage swapping to the real
  # image once the attachment-ready event fires -- both in a real webview
  # driven through WebDriver, not in a unit test's fake DOM.
  Scenario: An attachment not yet in the local cache shows a placeholder, then the real image
    Given Alice is a device in the space, online
    And Alice has sent a message with an attachment not yet present in her attachment store
    Then Alice's conversation view shows an attachment placeholder
    When the attachment bytes become available in Alice's attachment store
    Then Alice's conversation view shows the loaded attachment within 5 seconds
