from playwright.sync_api import sync_playwright
import time

def verify_frontend():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Navigate to a player profile page
        # Using a dummy user ID that might not have much data, but we just want to see the button behavior
        # Assuming the app is running on localhost:5000
        page.goto("http://127.0.0.1:5000/user/12345")

        # Take initial screenshot
        page.screenshot(path="verification/initial_state.png")
        print("Initial state screenshot taken.")

        # Wait for any "No match history" message or similar to ensure load
        # Find the "Load Older History" button (might be hidden if fetch_status is complete)
        # To force it to appear, we might need a user with partial history.
        # However, for verification, we just want to check if the script runs and button exists if applicable.

        # Since we don't have real data populated for 12345, the template might show "No match history found locally".
        # We need to simulate a state where the button is visible.
        # But we can't easily modify the backend state from here without complex setup.

        # Let's try to verify that the JS is loaded by checking for the polling function in global scope?
        # Or just checking if the page loads without JS errors.

        # Check for console errors
        page.on("console", lambda msg: print(f"Console log: {msg.text}"))
        page.on("pageerror", lambda exc: print(f"Page error: {exc}"))

        # Reload to trigger the DOMContentLoaded event handler
        page.reload()
        time.sleep(1)

        # Take another screenshot
        page.screenshot(path="verification/loaded_state.png")
        print("Loaded state screenshot taken.")

        browser.close()

if __name__ == "__main__":
    verify_frontend()
