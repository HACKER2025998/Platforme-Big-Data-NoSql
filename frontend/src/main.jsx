import React, { useState, useEffect, useCallback, useRef } from "react";
import { createRoot } from "react-dom/client";
import {
  Thermometer,
  LayoutDashboard,
  Radio,
  Clock3,
  Bell,
  ArrowUpRight,
  Plus,
  ChevronRight,
  RefreshCw,
  Check,
  SlidersHorizontal,
  Activity,
  X,
  Download,
  Menu,
} from "lucide-react";
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceLine,
} from "recharts";
import { request, send } from "./api";
import "./style.css";
const fmt = (n) =>
  n == null
    ? "—"
    : Number(n).toLocaleString("fr-FR", { maximumFractionDigits: 1 });
const time = (s) =>
  new Date(s).toLocaleTimeString("fr-FR", {
    hour: "2-digit",
    minute: "2-digit",
  });
const date = (s) =>
  new Date(s).toLocaleString("fr-FR", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  });
function App() {
  const [tab, setTab] = useState("Vue d’ensemble"),
    [sensors, setSensors] = useState([]),
    [latest, setLatest] = useState({}),
    [alerts, setAlerts] = useState([]),
    [selected, setSelected] = useState(""),
    [history, setHistory] = useState([]),
    [stats, setStats] = useState(null),
    [period, setPeriod] = useState(1),
    [error, setError] = useState(""),
    [notice, setNotice] = useState(""),
    [synced, setSynced] = useState(null),
    [modal, setModal] = useState(null),
    [busy, setBusy] = useState(false),
    [receipt, setReceipt] = useState(null),
    [filter, setFilter] = useState("OPEN"),
    [page, setPage] = useState(0),
    [total, setTotal] = useState(0),
    [sensorPage, setSensorPage] = useState(0),
    [sensorTotal, setSensorTotal] = useState(0),
    [cursor, setCursor] = useState(null),
    [mobile, setMobile] = useState(false);
  useEffect(() => {
    if (!modal) return;
    const previous = document.activeElement;
    const handler = (event) => {
      if (event.key === "Escape") {
        setModal(null);
        return;
      }
      if (event.key !== "Tab") return;
      const fields = [
        ...document.querySelectorAll(
          ".modal button, .modal input, .modal select",
        ),
      ].filter((x) => !x.disabled);
      const first = fields[0],
        last = fields[fields.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last?.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first?.focus();
      }
    };
    document.addEventListener("keydown", handler);
    return () => {
      document.removeEventListener("keydown", handler);
      previous?.focus();
    };
  }, [modal]);
  const active = useRef(true),
    refreshing = useRef(false);
  const current = sensors.find((x) => x.id === selected);
  const refresh = useCallback(async () => {
    if (refreshing.current) return;
    refreshing.current = true;
    try {
      const [s, a] = await Promise.all([
        request(`/capteurs?taille=100&page=${sensorPage}`),
        request(
          `/alertes?taille=10&page=${page}${filter ? "&statut=" + filter : ""}`,
        ),
      ]);
      const values = await Promise.all(
        s.items.map(async (x) => [
          x.id,
          await request(`/capteurs/${x.id}/derniere-mesure`),
        ]),
      );
      if (!active.current) return;
      setSensors(s.items);
      setSensorTotal(s.total);
      setAlerts(a.items);
      setTotal(a.total);
      setLatest(Object.fromEntries(values));
      setSelected((prev) =>
        s.items.some((x) => x.id === prev) ? prev : s.items[0]?.id || "",
      );
      setSynced(new Date());
      setError("");
    } catch (e) {
      if (active.current) setError(e.message);
    } finally {
      refreshing.current = false;
    }
  }, [filter, page, sensorPage]);
  useEffect(() => {
    active.current = true;
    refresh();
    const i = setInterval(refresh, 2000);
    return () => {
      active.current = false;
      clearInterval(i);
    };
  }, [refresh]);
  const loadHistory = useCallback(
    async (next = null) => {
      if (!selected) {
        setHistory([]);
        setStats(null);
        return;
      }
      const au = new Date(),
        du = new Date(au - period * 3600000);
      const params = `du=${encodeURIComponent(du.toISOString())}&au=${encodeURIComponent(au.toISOString())}`;
      try {
        const [h, s] = await Promise.all([
          request(`/capteurs/${selected}/mesures?${params}&limite=100`),
          request(`/capteurs/${selected}/statistiques?${params}`),
        ]);
        setHistory(h.items);
        setCursor(h.nextCursor ? { token: h.nextCursor, params } : null);
        setStats(s);
      } catch (e) {
        setError(e.message);
      }
    },
    [selected, period],
  );
  useEffect(() => {
    loadHistory();
    const i = setInterval(() => {
      if (tab !== "Historique") loadHistory();
    }, 4000);
    return () => clearInterval(i);
  }, [loadHistory, tab]);
  useEffect(() => {
    if (!receipt || ["PROCESSED", "FAILED"].includes(receipt.processingState))
      return;
    const i = setInterval(async () => {
      try {
        setReceipt(await request("/ingestions/" + receipt.id));
      } catch (e) {
        setError(e.message);
      }
    }, 2000);
    return () => clearInterval(i);
  }, [receipt]);
  useEffect(() => {
    if (notice) {
      const t = setTimeout(() => setNotice(""), 5000);
      return () => clearTimeout(t);
    }
  }, [notice]);
  async function act(fn) {
    setBusy(true);
    try {
      await fn();
      await refresh();
    } catch (e) {
      setError(e.message);
    } finally {
      setBusy(false);
    }
  }
  async function save(event) {
    event.preventDefault();
    const f = Object.fromEntries(new FormData(event.target));
    await act(async () => {
      if (modal.type === "measure") {
        const r = await send("/mesures", {
          eventId: crypto.randomUUID(),
          capteurId: f.capteurId,
          date: new Date().toISOString(),
          temperature: Number(f.temperature),
        });
        setReceipt(r);
        setNotice("Mesure reçue. Le traitement se poursuit.");
      } else if (modal.type === "edit") {
        await send(
          "/capteurs/" + modal.sensor.id,
          {
            libelle: f.libelle,
            salle: f.salle,
            seuilTemperature: Number(f.seuilTemperature),
            actif: f.actif === "on",
            version: modal.sensor.version,
          },
          "PATCH",
        );
        setNotice("Capteur mis à jour.");
      } else {
        await send("/capteurs", {
          ...f,
          seuilTemperature: Number(f.seuilTemperature),
          actif: true,
        });
        setNotice("Capteur ajouté.");
      }
      setModal(null);
    });
  }
  async function more() {
    if (!cursor) return;
    try {
      const h = await request(
        `/capteurs/${selected}/mesures?${cursor.params}&limite=100&curseur=${encodeURIComponent(cursor.token)}`,
      );
      setHistory((v) => [...v, ...h.items]);
      setCursor(h.nextCursor ? { ...cursor, token: h.nextCursor } : null);
    } catch (e) {
      setError(e.message);
    }
  }
  function exportCsv() {
    const rows = [
      "date,capteurId,eventId,temperature",
      ...history.map(
        (x) => `${x.date},${x.capteurId},${x.eventId},${x.temperature}`,
      ),
    ];
    const url = URL.createObjectURL(
      new Blob([rows.join("\n")], { type: "text/csv;charset=utf-8" }),
    );
    const a = document.createElement("a");
    a.href = url;
    a.download = `historique-${selected}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }
  const vals = Object.values(latest).filter(Boolean),
    over = vals.filter(
      (x) =>
        Number(x.temperature) >
        Number(sensors.find((s) => s.id === x.capteurId)?.seuilTemperature),
    ),
    average = vals.length
      ? vals.reduce((a, x) => a + Number(x.temperature), 0) / vals.length
      : null;
  const nav = [
    ["Vue d’ensemble", LayoutDashboard],
    ["Capteurs", Radio],
    ["Historique", Clock3],
    ["Alertes", Bell],
  ];
  const picker = (
    <div className="chart-controls">
      <select
        aria-label="Capteur"
        value={selected}
        onChange={(e) => setSelected(e.target.value)}
      >
        {sensors.map((s) => (
          <option key={s.id} value={s.id}>
            {s.salle} · {s.id}
          </option>
        ))}
      </select>
      <select
        aria-label="Période"
        value={period}
        onChange={(e) => setPeriod(Number(e.target.value))}
      >
        <option value="1">Dernière heure</option>
        <option value="6">6 dernières heures</option>
        <option value="24">24 dernières heures</option>
        <option value="168">7 derniers jours</option>
      </select>
    </div>
  );
  return (
    <div className="app">
      <aside className={mobile ? "sidebar open" : "sidebar"}>
        <a
          className="brand"
          href="#"
          onClick={(e) => {
            e.preventDefault();
            setTab("Vue d’ensemble");
          }}
        >
          <span className="brand-icon">
            <Thermometer size={23} />
          </span>
          thermo<span className="brand-dot">.</span>
        </a>
        <div className="workspace">
          <div className="workspace-logo">P</div>
          <div>
            <strong>Plateforme températures</strong>
            <small>Espace de supervision</small>
          </div>
        </div>
        <div className="nav-label">SUPERVISION</div>
        <nav>
          {nav.map(([name, Icon]) => (
            <button
              className={tab === name ? "nav-item active" : "nav-item"}
              key={name}
              onClick={() => {
                setTab(name);
                setMobile(false);
              }}
            >
              <Icon size={19} />
              <span>{name}</span>
              {name === "Alertes" && filter === "OPEN" && total > 0 && (
                <span className="nav-count">{total}</span>
              )}
            </button>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <div className="lab-status">
            <span className={error ? "dot offline" : "dot"} />
            <div>
              <strong>
                {error ? "Connexion interrompue" : "Supervision en direct"}
              </strong>
              <small>Actualisation toutes les 2 s</small>
            </div>
          </div>
          <div className="profile">
            <div className="avatar">EQ</div>
            <div>
              <strong>Équipe projet</strong>
              <small>Laboratoire Big Data & NoSQL</small>
            </div>
          </div>
        </div>
      </aside>
      <div className="body">
        <header className="topbar">
          <button
            className="mobile-toggle icon-button"
            aria-label="Menu"
            onClick={() => setMobile(!mobile)}
          >
            <Menu size={20} />
          </button>
          <div className="breadcrumb">
            Supervision <ChevronRight size={14} />
            <span>{tab}</span>
          </div>
          <div className="top-right">
            <span className="environment">Environnement laboratoire</span>
            <div className="avatar small">EQ</div>
          </div>
        </header>
        <main>
          <div className="page-head">
            <div>
              <div className="eyebrow">PLATEFORME DE TEMPÉRATURES</div>
              <h1>{tab}</h1>
              <p>
                Suivez vos salles, comprenez les variations et intervenez au bon
                moment.
              </p>
            </div>
            <button
              className="primary"
              onClick={() => setModal({ type: "create" })}
            >
              <Plus size={17} />
              Ajouter un capteur
            </button>
          </div>
          {error && (
            <div className="error" role="alert">
              {error}
              <button onClick={refresh}>Réessayer</button>
            </div>
          )}
          {notice && (
            <div className="notice" role="status">
              <Check size={16} />
              {notice}
            </div>
          )}
          {receipt && (
            <div className="receipt">
              <Activity size={16} />
              <span>
                Dernière ingestion :{" "}
                <strong>
                  {receipt.processingState === "PROCESSED"
                    ? "Traitée"
                    : receipt.processingState === "FAILED"
                      ? "En échec"
                      : "En cours"}
                </strong>{" "}
                · publication{" "}
                {receipt.publicationState === "PUBLISHED"
                  ? "confirmée"
                  : "en attente"}
              </span>
              <code>{receipt.id.slice(0, 8)}</code>
              <button
                aria-label="Fermer le suivi"
                className="icon-button"
                onClick={() => setReceipt(null)}
              >
                <X size={15} />
              </button>
            </div>
          )}
          {tab === "Vue d’ensemble" && (
            <>
              <div className="summary">
                <Metric
                  label="Capteurs actifs"
                  value={sensors.filter((s) => s.actif).length}
                  suffix={"/ " + sensors.length}
                  icon={Radio}
                  note={
                    sensorTotal > 100
                      ? `Page de 100 sur ${sensorTotal} capteurs`
                      : "Capteurs de la page courante"
                  }
                />
                <Metric
                  label="Température moyenne"
                  value={fmt(average)}
                  suffix="°C"
                  icon={Thermometer}
                  note="Sur les dernières mesures reçues"
                />
                <Metric
                  label="Dépassements actuels"
                  value={over.length}
                  icon={Bell}
                  note={
                    over.length
                      ? "Une vérification est nécessaire"
                      : "Aucun seuil dépassé"
                  }
                />
                <Metric
                  label="Dernière synchronisation"
                  value={synced ? time(synced) : "—"}
                  icon={RefreshCw}
                  note={
                    synced
                      ? "Données actualisées automatiquement"
                      : "En attente de connexion"
                  }
                />
              </div>
              <div className="section-head">
                <h2>
                  Vos capteurs <span className="count">{sensorTotal}</span>
                </h2>
                <button
                  className="text-button"
                  onClick={() => setTab("Capteurs")}
                >
                  Gérer les capteurs
                  <ArrowUpRight size={16} />
                </button>
              </div>
              <div className="sensor-grid">
                {sensors.slice(0, 4).map((s) => (
                  <Sensor
                    key={s.id}
                    sensor={s}
                    value={latest[s.id]}
                    selected={s.id === selected}
                    onClick={() => setSelected(s.id)}
                  />
                ))}
                {!sensors.length && (
                  <Empty
                    title="Vos salles commencent ici"
                    text="Ajoutez un premier capteur pour commencer la supervision."
                  />
                )}
              </div>
            </>
          )}
          {(tab === "Vue d’ensemble" || tab === "Historique") && (
            <section className="panel history-panel">
              <div className="panel-head">
                <div>
                  <h2>Évolution de la température</h2>
                  <p>
                    {current
                      ? `${current.libelle} · ${current.salle}`
                      : "Sélectionnez un capteur pour consulter ses mesures"}
                  </p>
                </div>
                {picker}
              </div>
              <div className="chart-legend">
                <span>
                  <i />
                  Température
                </span>
                <span>
                  <i className="dashed" />
                  Seuil d’alerte{" "}
                  {current ? `${fmt(current.seuilTemperature)} °C` : ""}
                </span>
                <span className="unit">TEMPÉRATURE · °C</span>
              </div>
              <div className="chart">
                {history.length ? (
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart
                      data={[...history].reverse()}
                      margin={{ top: 16, right: 24, left: -20, bottom: 0 }}
                    >
                      <defs>
                        <linearGradient id="fill" x1="0" y1="0" x2="0" y2="1">
                          <stop
                            offset="0%"
                            stopColor="#347367"
                            stopOpacity={0.18}
                          />
                          <stop
                            offset="100%"
                            stopColor="#347367"
                            stopOpacity={0.01}
                          />
                        </linearGradient>
                      </defs>
                      <CartesianGrid
                        strokeDasharray="3 5"
                        vertical={false}
                        stroke="#e7ece9"
                      />
                      <XAxis
                        dataKey="date"
                        tickFormatter={time}
                        tickLine={false}
                        axisLine={false}
                        minTickGap={60}
                        tick={{ fill: "#84918c", fontSize: 11 }}
                      />
                      <YAxis
                        tickLine={false}
                        axisLine={false}
                        domain={[
                          "auto",
                          (dataMax) =>
                            Math.max(
                              dataMax + 3,
                              Number(current?.seuilTemperature) || 40,
                            ),
                        ]}
                        tick={{ fill: "#84918c", fontSize: 11 }}
                      />
                      <Tooltip
                        labelFormatter={date}
                        formatter={(v) => [`${fmt(v)} °C`, "Température"]}
                        contentStyle={{
                          border: "1px solid #e0e8e3",
                          borderRadius: 8,
                          fontSize: 12,
                        }}
                      />
                      <ReferenceLine
                        y={Number(current?.seuilTemperature) || 40}
                        stroke="#a9b5ad"
                        strokeDasharray="5 5"
                      />
                      <Area
                        type="monotone"
                        dataKey="temperature"
                        stroke="#347367"
                        strokeWidth={2}
                        fill="url(#fill)"
                        isAnimationActive={false}
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                ) : (
                  <Empty
                    title="Aucune mesure sur cette période"
                    text="Envoyez une mesure ou lancez le générateur de démonstration."
                  />
                )}
              </div>
              <div className="chart-footer">
                <span>
                  <span className="dot" />
                  Historique des mesures reçues
                </span>
                <div>
                  <span>
                    Min. <strong>{fmt(stats?.min)} °C</strong>
                  </span>
                  <span>
                    Max. <strong>{fmt(stats?.max)} °C</strong>
                  </span>
                  <span>
                    Moy. <strong>{fmt(stats?.moyenne)} °C</strong>
                  </span>
                </div>
              </div>
            </section>
          )}
          {tab === "Capteurs" && (
            <section className="panel">
              <div className="panel-head">
                <div>
                  <h2>Parc de capteurs</h2>
                  <p>Configuration, seuils et état des salles surveillées.</p>
                </div>
                <button
                  className="secondary"
                  onClick={() => setModal({ type: "measure" })}
                >
                  <Plus size={16} />
                  Envoyer une mesure
                </button>
              </div>
              <div className="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>CAPTEUR</th>
                      <th>SALLE</th>
                      <th>DERNIÈRE VALEUR</th>
                      <th>SEUIL</th>
                      <th>ÉTAT</th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    {sensors.map((s) => (
                      <tr key={s.id}>
                        <td>
                          <strong>{s.libelle}</strong>
                          <small>{s.id}</small>
                        </td>
                        <td>{s.salle}</td>
                        <td>{fmt(latest[s.id]?.temperature)} °C</td>
                        <td>{fmt(s.seuilTemperature)} °C</td>
                        <td>
                          <span className="badge">
                            {s.actif ? "Actif" : "Inactif"}
                          </span>
                        </td>
                        <td>
                          <button
                            className="secondary"
                            onClick={() =>
                              setModal({ type: "edit", sensor: s })
                            }
                          >
                            <SlidersHorizontal size={14} />
                            Modifier
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {!sensors.length && (
                <Empty
                  title="Aucun capteur"
                  text="Utilisez le bouton Ajouter un capteur."
                />
              )}
              <div className="pagination">
                <span>{sensorTotal} capteurs</span>
                <div>
                  <button
                    disabled={!sensorPage}
                    onClick={() => setSensorPage((p) => p - 1)}
                  >
                    Précédent
                  </button>
                  <span>{sensorPage + 1}</span>
                  <button
                    disabled={(sensorPage + 1) * 100 >= sensorTotal}
                    onClick={() => setSensorPage((p) => p + 1)}
                  >
                    Suivant
                  </button>
                </div>
              </div>
            </section>
          )}
          {tab === "Historique" && (
            <section className="panel">
              <div className="panel-head">
                <div>
                  <h2>Journal des mesures</h2>
                  <p>
                    Ordre chronologique décroissant · dates affichées en heure
                    locale.
                  </p>
                </div>
                <button
                  className="secondary"
                  disabled={!history.length}
                  onClick={exportCsv}
                >
                  <Download size={16} />
                  Exporter CSV
                </button>
              </div>
              <div className="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>DATE</th>
                      <th>CAPTEUR</th>
                      <th>TEMPÉRATURE</th>
                      <th>ÉVÉNEMENT</th>
                    </tr>
                  </thead>
                  <tbody>
                    {history.map((m) => (
                      <tr key={m.eventId}>
                        <td>{date(m.date)}</td>
                        <td>{m.capteurId}</td>
                        <td>
                          <strong>{fmt(m.temperature)} °C</strong>
                        </td>
                        <td>
                          <code>{m.eventId}</code>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {cursor && (
                <div className="pagination">
                  <button className="secondary" onClick={more}>
                    Charger la suite
                  </button>
                </div>
              )}
            </section>
          )}
          {(tab === "Vue d’ensemble" || tab === "Alertes") && (
            <section className="panel alerts-panel">
              <div className="panel-head">
                <div>
                  <h2>
                    Alertes {tab === "Vue d’ensemble" ? "récentes" : ""}
                    <span className="count">{total}</span>
                  </h2>
                  <p>
                    Les dépassements de seuil qui demandent votre attention.
                  </p>
                </div>
                <select
                  aria-label="Statut des alertes"
                  value={filter}
                  onChange={(e) => {
                    setFilter(e.target.value);
                    setPage(0);
                  }}
                >
                  <option value="OPEN">À acquitter</option>
                  <option value="ACKNOWLEDGED">Acquittées</option>
                  <option value="">Toutes les alertes</option>
                </select>
              </div>
              <div className="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>CAPTEUR</th>
                      <th>TEMPÉRATURE</th>
                      <th>SEUIL APPLIQUÉ</th>
                      <th>DÉTECTION</th>
                      <th>STATUT</th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    {alerts.map((a) => (
                      <tr key={a.id}>
                        <td>
                          <div className="table-sensor">
                            <span className="table-icon">
                              <Thermometer size={16} />
                            </span>
                            <div>
                              <strong>
                                {sensors.find((s) => s.id === a.capteurId)
                                  ?.salle || a.capteurId}
                              </strong>
                              <small>{a.capteurId}</small>
                            </div>
                          </div>
                        </td>
                        <td>
                          <strong>{fmt(a.temperature)} °C</strong>
                        </td>
                        <td>{fmt(a.seuilApplique)} °C</td>
                        <td>{date(a.date)}</td>
                        <td>
                          <span
                            className={
                              "badge " + (a.statut === "OPEN" ? "warning" : "")
                            }
                          >
                            <span className="mini-dot" />
                            {a.statut === "OPEN" ? "À acquitter" : "Acquittée"}
                          </span>
                        </td>
                        <td>
                          {a.statut === "OPEN" ? (
                            <button
                              className="ack"
                              disabled={busy}
                              onClick={() =>
                                act(async () => {
                                  await send(
                                    `/alertes/${a.id}/acquittement`,
                                    undefined,
                                    "PATCH",
                                  );
                                  setNotice("Alerte acquittée.");
                                })
                              }
                            >
                              <Check size={15} />
                              Acquitter
                            </button>
                          ) : (
                            <span className="muted">
                              {a.dateAcquittement
                                ? date(a.dateAcquittement)
                                : "—"}
                            </span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {!alerts.length && (
                <Empty
                  title="Aucune alerte à afficher"
                  text="Les dépassements de seuil apparaîtront ici après traitement."
                />
              )}
              <div className="pagination">
                <span>
                  {total} alerte{total !== 1 ? "s" : ""}
                </span>
                <div>
                  <button
                    disabled={!page}
                    onClick={() => setPage((p) => p - 1)}
                  >
                    Précédent
                  </button>
                  <span>{page + 1}</span>
                  <button
                    disabled={(page + 1) * 10 >= total}
                    onClick={() => setPage((p) => p + 1)}
                  >
                    Suivant
                  </button>
                </div>
              </div>
            </section>
          )}
          <footer>
            <span>Thermo · Plateforme de surveillance des températures</span>
            <span>
              Big Data & NoSQL <span className="footer-separator">/</span>{" "}
              Équipe 4
            </span>
          </footer>
        </main>
      </div>
      {modal && (
        <div
          className="overlay"
          onMouseDown={(e) => {
            if (e.target === e.currentTarget) setModal(null);
          }}
        >
          <section
            role="dialog"
            aria-modal="true"
            aria-labelledby="modal-title"
            className="modal"
          >
            <div className="modal-head">
              <div>
                <div className="eyebrow">SUPERVISION</div>
                <h2 id="modal-title">
                  {modal.type === "measure"
                    ? "Envoyer une mesure"
                    : modal.type === "edit"
                      ? "Modifier le capteur"
                      : "Nouveau capteur"}
                </h2>
              </div>
              <button
                className="icon-button"
                aria-label="Fermer"
                onClick={() => setModal(null)}
              >
                <X size={20} />
              </button>
            </div>
            {error && (
              <div className="error" role="alert">
                {error}
              </div>
            )}
            <form onSubmit={save}>
              {modal.type === "measure" ? (
                <>
                  <label>
                    Capteur
                    <select name="capteurId" required>
                      {sensors
                        .filter((s) => s.actif)
                        .map((s) => (
                          <option key={s.id} value={s.id}>
                            {s.salle} · {s.id}
                          </option>
                        ))}
                    </select>
                  </label>
                  <label>
                    Température (°C)
                    <input
                      autoFocus
                      name="temperature"
                      type="number"
                      min="-50"
                      max="100"
                      step="0.01"
                      defaultValue="24"
                      required
                    />
                  </label>
                  <p className="form-note">
                    La mesure sera horodatée à l’envoi. Son état de traitement
                    apparaîtra dans le suivi.
                  </p>
                </>
              ) : (
                <>
                  {modal.type === "create" && (
                    <label>
                      Identifiant
                      <input
                        autoFocus
                        name="id"
                        placeholder="SALLE-01"
                        pattern="[A-Z0-9_-]{1,64}"
                        maxLength="64"
                        required
                      />
                      <small>
                        Majuscules, chiffres, tirets et underscores.
                      </small>
                    </label>
                  )}
                  <label>
                    Libellé
                    <input
                      name="libelle"
                      defaultValue={modal.sensor?.libelle}
                      maxLength="120"
                      required
                      placeholder="Capteur principal"
                    />
                  </label>
                  <label>
                    Salle
                    <input
                      name="salle"
                      defaultValue={modal.sensor?.salle}
                      maxLength="120"
                      required
                      placeholder="Salle de réunion"
                    />
                  </label>
                  <label>
                    Seuil d’alerte (°C)
                    <input
                      name="seuilTemperature"
                      type="number"
                      step="0.01"
                      min="-50"
                      max="100"
                      defaultValue={modal.sensor?.seuilTemperature ?? 40}
                      required
                    />
                  </label>
                  {modal.type === "edit" && (
                    <label className="checkbox">
                      <input
                        name="actif"
                        type="checkbox"
                        defaultChecked={modal.sensor.actif}
                      />
                      Capteur actif
                    </label>
                  )}
                </>
              )}
              <div className="form-actions">
                <button
                  type="button"
                  className="secondary"
                  onClick={() => setModal(null)}
                >
                  Annuler
                </button>
                <button className="primary" disabled={busy}>
                  {busy
                    ? "Enregistrement…"
                    : modal.type === "measure"
                      ? "Envoyer la mesure"
                      : "Enregistrer"}
                </button>
              </div>
            </form>
          </section>
        </div>
      )}
    </div>
  );
}
function Metric({ label, value, suffix, icon: Icon, note }) {
  return (
    <div className="metric">
      <div className="metric-label">
        {label}
        <Icon size={17} />
      </div>
      <div className="metric-value">
        {value}
        <span>{suffix}</span>
      </div>
      <p>{note}</p>
    </div>
  );
}
function Sensor({ sensor: s, value, selected, onClick }) {
  const high = value && Number(value.temperature) > s.seuilTemperature;
  return (
    <button
      className={"sensor " + (selected ? "selected" : "")}
      onClick={onClick}
    >
      <div className="sensor-top">
        <span className="sensor-icon">
          <Thermometer size={19} />
        </span>
        <span className={"sensor-state " + (high ? "high" : "")}>
          <span className="mini-dot" />
          {!s.actif
            ? "Inactif"
            : !value
              ? "En attente"
              : high
                ? "Seuil dépassé"
                : "Normal"}
        </span>
      </div>
      <h3>{s.salle}</h3>
      <span className="sensor-id">{s.id}</span>
      <div className="sensor-value">
        {fmt(value?.temperature)}
        <span>°C</span>
      </div>
      <div className="sensor-bottom">
        <span>Seuil : {fmt(s.seuilTemperature)} °C</span>
        <span>{value ? time(value.date) : "Aucune mesure"}</span>
      </div>
    </button>
  );
}
function Empty({ title, text }) {
  return (
    <div className="empty">
      <Activity size={24} />
      <strong>{title}</strong>
      <p>{text}</p>
    </div>
  );
}
createRoot(document.getElementById("root")).render(<App />);
