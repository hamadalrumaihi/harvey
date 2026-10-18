"""The user's "no" to a business sticks, and a site that already credits a
photographer is flagged on the draft (never auto-skipped)."""

import os
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from harvey.collectors.profile import ProfileCollector
from harvey.models.campaign import Campaign, EmailStep
from harvey.models.company import Company
from harvey.models.prospect import Prospect
from harvey.signals import seed_signal_catalog
from harvey.state import StateManager

NOW = "2026-10-18T09:00:00"


@pytest_asyncio.fixture
async def state():
    with tempfile.TemporaryDirectory() as tmp:
        sm = StateManager(os.path.join(tmp, "t.db"))
        await sm.init_db()
        yield sm


async def seed_gym(state, name="Doha BJJ", domain="dohabjj.qa", people=("coach", "owner")):
    cid = await state.add_company(Company(name=name, domain=domain))
    pids = []
    for who in people:
        pids.append(await state.add_prospect(Prospect(
            first_name=who.title(), last_name="X", company=name, company_id=cid,
            email=f"{who}@{domain}", email_status="verified", email_verified=True,
            status="queued",
        )))
    return cid, pids


async def stage(state, pids, kind="sequence"):
    campaign_id = await state.add_campaign(Campaign(
        id="", name="c", channel="email", prospect_ids=pids, status="active",
        sequence=[EmailStep(step=1, subject="s", body="b", delay_days=0)],
    ))
    items = {}
    for pid in pids:
        for step in (1, 2, 3):
            items[(pid, step)] = await state.add_outbox_item(
                prospect_id=pid, campaign_id=campaign_id, step=step, kind=kind,
                to_email=f"{pid}@x.co", subject="s", body="b", send_at=NOW,
                status="approved" if step == 3 else "pending_review",
            )
    return items


# ── rejecting a cold email skips the whole business ──

@pytest.mark.asyncio
async def test_reject_skips_business_and_cancels_its_queue(state):
    cid, (coach, owner) = await seed_gym(state)
    _, (other,) = await seed_gym(state, name="Other Gym", domain="other.qa", people=("x",))
    items = await stage(state, [coach, owner, other])

    res = await state.reject_outbox_item(items[(coach, 1)])

    assert res == {"rejected": True, "company": "Doha BJJ", "cancelled": 5}
    assert (await state.get_outbox_item(items[(coach, 1)]))["status"] == "rejected"
    for key in [(coach, 2), (coach, 3), (owner, 1), (owner, 2), (owner, 3)]:
        assert (await state.get_outbox_item(items[key]))["status"] == "cancelled"
    assert (await state.get_prospect(coach)).status == "skipped"
    assert (await state.get_prospect(owner)).status == "skipped"
    # A different business is untouched.
    assert (await state.get_outbox_item(items[(other, 1)]))["status"] == "pending_review"
    assert (await state.get_prospect(other)).status == "queued"


@pytest.mark.asyncio
async def test_people_found_later_at_a_skipped_business_arrive_skipped(state):
    cid, (coach, _) = await seed_gym(state)
    items = await stage(state, [coach])
    await state.reject_outbox_item(items[(coach, 1)])

    late = await state.add_prospect(Prospect(
        first_name="New", last_name="Hire", company="Doha BJJ", company_id=cid,
        email="new@dohabjj.qa", email_status="verified", status="new",
    ))
    assert (await state.get_prospect(late)).status == "skipped"
    assert all(p.id != late for p in await state.get_prospects_by_status("new"))


@pytest.mark.asyncio
async def test_replied_prospect_keeps_status_when_business_skipped(state):
    _, (coach, owner) = await seed_gym(state)
    await state.update_prospect_status(owner, "replied")
    items = await stage(state, [coach])
    await state.reject_outbox_item(items[(coach, 1)])
    assert (await state.get_prospect(owner)).status == "replied"


@pytest.mark.asyncio
async def test_rejecting_a_reply_draft_does_not_skip_the_business(state):
    _, (coach, owner) = await seed_gym(state)
    reply = await state.add_outbox_item(
        prospect_id=coach, kind="reply", to_email="coach@dohabjj.qa",
        subject="re", body="b", send_at=NOW, status="pending_review",
    )
    res = await state.reject_outbox_item(reply)
    assert res["rejected"] and res["company"] == ""
    assert (await state.get_prospect(coach)).status == "queued"
    assert (await state.get_prospect(owner)).status == "queued"


@pytest.mark.asyncio
async def test_reject_unknown_or_already_sent_item_is_a_noop(state):
    _, (coach,) = await seed_gym(state, people=("coach",))
    items = await stage(state, [coach])
    await state.update_outbox_item(items[(coach, 1)], status="sent")
    assert (await state.reject_outbox_item(items[(coach, 1)]))["rejected"] is False
    assert (await state.reject_outbox_item("nope"))["rejected"] is False
    assert (await state.get_prospect(coach)).status == "queued"


@pytest.mark.asyncio
async def test_restore_undoes_the_skip(state):
    cid, (coach, owner) = await seed_gym(state)
    items = await stage(state, [coach])
    await state.reject_outbox_item(items[(coach, 1)])

    res = await state.restore_skipped_business(items[(coach, 1)])

    assert res == {"restored": 2, "company": "Doha BJJ"}
    assert (await state.get_prospect(coach)).status == "new"
    assert (await state.get_prospect(owner)).status == "new"
    later = await state.add_prospect(Prospect(
        first_name="A", last_name="B", company_id=cid, email="a@dohabjj.qa", status="new",
    ))
    assert (await state.get_prospect(later)).status == "new"


@pytest.mark.asyncio
async def test_sender_never_sends_to_a_skipped_prospect(state):
    from harvey.agents.sender import STOP_STATUSES
    assert "skipped" in STOP_STATUSES


# ── existing-photographer detection ──

def collector(confirmed=None):
    c = ProfileCollector.__new__(ProfileCollector)
    c.confirmed = confirmed or set()
    return c


@pytest.mark.parametrize("html,expect", [
    ("<footer>Photos by <a>Doha Lens Studio</a></footer>", "Photos by Doha Lens Studio"),
    ("<p>Photos&nbsp;by Mo</p>", "Photos by Mo"),
    ("<p>Meet our official photographer, Ali</p>", "official photographer"),
    ("<p>Our in-house media team covers every event</p>", "in-house media team"),
    ("<p>📸 @fightshots</p>", "📸 @fightshots"),
    ("<p>عدسة: @qatar_lens</p>", "عدسة: @qatar_lens"),
    ('<a href="https://club.pixieset.com/2026">Tournament gallery</a>', "galleries on pixieset.com"),
])
def test_photographer_detected(html, expect):
    got = collector().detect_photographer(html)
    assert got is not None and got[0] == expect


@pytest.mark.parametrize("html", [
    "<p>Kids BJJ classes. Photography day this Saturday!</p>",
    "<p>Book a private session with our coaches.</p>",
    '<a href="https://instagram.com/dohabjj">Instagram</a>',
    '<a href="https://notpic-time.com.evil.io/x">x</a>',
])
def test_no_photographer_on_plain_gym_pages(html):
    assert collector().detect_photographer(html) is None


@pytest.mark.asyncio
async def test_profile_checks_gallery_page_and_flags_the_draft(state):
    await seed_signal_catalog(state)
    await state.set_signal_status("HAS_PHOTOGRAPHER", "confirmed")
    c = collector(await state.confirmed_signal_codes())
    c.state = state

    pages = {
        "https://dohabjj.qa": "<html><h1>Doha BJJ</h1>" + "x" * 600 + "</html>",
        "https://dohabjj.qa/gallery": "<html>" + "y" * 600 + "<p>Photo credit: Gulf Grapple Media</p></html>",
    }

    async def fake_get(url):
        return pages.get(url.rstrip("/"), "")
    c._get = fake_get

    cid, (coach,) = await seed_gym(state, people=("coach",))
    obs = await c.profile_company(cid, "dohabjj.qa")
    hit = [o for o in obs if o["signal_code"] == "HAS_PHOTOGRAPHER"]
    assert len(hit) == 1 and hit[0]["value_num"] == 1
    assert "Gulf Grapple Media" in hit[0]["value_text"]
    assert hit[0]["evidence_url"].endswith("/gallery")

    await state.add_observations(obs)
    flags = await state.outbox_business_flags([coach])
    assert flags[coach]["company"] == "Doha BJJ"
    assert "Gulf Grapple Media" in flags[coach]["photographer"]


@pytest.mark.asyncio
async def test_clean_site_records_a_negative_and_no_flag(state):
    await seed_signal_catalog(state)
    await state.set_signal_status("HAS_PHOTOGRAPHER", "confirmed")
    c = collector(await state.confirmed_signal_codes())
    c.state = state

    async def fake_get(url):
        return "<html><h1>Doha BJJ</h1>" + "x" * 600 + "</html>" if url.rstrip("/").endswith(".qa") else ""
    c._get = fake_get

    cid, (coach,) = await seed_gym(state, people=("coach",))
    obs = await c.profile_company(cid, "dohabjj.qa")
    assert [o["value_num"] for o in obs if o["signal_code"] == "HAS_PHOTOGRAPHER"] == [0]
    await state.add_observations(obs)
    assert (await state.outbox_business_flags([coach]))[coach]["photographer"] == ""


# ── dashboard ──

@pytest.fixture
def client(monkeypatch):
    import harvey.dashboard as dash
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "harvey.db"
        monkeypatch.setattr(dash, "DB_PATH", db)
        monkeypatch.setattr(dash, "WEB_DIR", Path(__file__).resolve().parent.parent / "harvey" / "web")
        with TestClient(dash.app) as c:
            c.db_path = db
            yield c


def test_dashboard_reject_skips_and_restore_undoes(client):
    import asyncio

    async def seed():
        sm = StateManager(str(client.db_path))
        await sm.init_db()
        await seed_signal_catalog(sm)
        cid, pids = await seed_gym(sm)
        await sm.add_observation(
            company_id=cid, signal_code="HAS_PHOTOGRAPHER", collector="profile",
            value_num=1, value_text="Photos by Doha Lens",
        )
        return await stage(sm, pids), pids

    items, (coach, _) = asyncio.run(seed())

    pending = client.get("/api/outbox").json()["pending"]
    first = next(i for i in pending if i["id"] == items[(coach, 1)])
    assert first["company"] == "Doha BJJ"
    assert first["photographer"] == "Photos by Doha Lens"

    res = client.post(f"/api/outbox/{items[(coach, 1)]}/reject").json()
    assert res["success"] and res["company"] == "Doha BJJ" and res["cancelled"] == 5
    assert client.get("/api/outbox").json()["pending"] == []

    res = client.post(f"/api/outbox/{items[(coach, 1)]}/restore").json()
    assert res["success"] and res["restored"] == 2
