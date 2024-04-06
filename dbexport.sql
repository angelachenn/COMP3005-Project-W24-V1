--
-- PostgreSQL database dump
--

-- Dumped from database version 13.4
-- Dumped by pg_dump version 13.4

-- Started on 2024-03-15 13:43:39

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 2 (class 3079 OID 16384)
-- Name: adminpack; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS adminpack WITH SCHEMA pg_catalog;


--
-- TOC entry 3039 (class 0 OID 0)
-- Dependencies: 2
-- Name: EXTENSION adminpack; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION adminpack IS 'administrative functions for PostgreSQL';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 206 (class 1259 OID 16412)
-- Name: matches; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.matches (
    match_id integer NOT NULL,
    season character varying(9) NOT NULL
);


ALTER TABLE public.matches OWNER TO postgres;

--
-- TOC entry 205 (class 1259 OID 16410)
-- Name: matches_match_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.matches_match_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.matches_match_id_seq OWNER TO postgres;

--
-- TOC entry 3040 (class 0 OID 0)
-- Dependencies: 205
-- Name: matches_match_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.matches_match_id_seq OWNED BY public.matches.match_id;


--
-- TOC entry 202 (class 1259 OID 16396)
-- Name: players; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.players (
    player_id integer NOT NULL,
    player_name character varying(255) NOT NULL
);


ALTER TABLE public.players OWNER TO postgres;

--
-- TOC entry 201 (class 1259 OID 16394)
-- Name: players_player_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.players_player_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.players_player_id_seq OWNER TO postgres;

--
-- TOC entry 3041 (class 0 OID 0)
-- Dependencies: 201
-- Name: players_player_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.players_player_id_seq OWNED BY public.players.player_id;


--
-- TOC entry 208 (class 1259 OID 16420)
-- Name: playerstatistics; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.playerstatistics (
    stat_id integer NOT NULL,
    match_id integer,
    player_id integer,
    shots integer,
    first_time_shots integer,
    through_balls integer,
    successful_dribbles integer,
    dribbled_past integer,
    intended_recipient_of_passes integer,
    average_xg numeric(5,2)
);


ALTER TABLE public.playerstatistics OWNER TO postgres;

--
-- TOC entry 207 (class 1259 OID 16418)
-- Name: playerstatistics_stat_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.playerstatistics_stat_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.playerstatistics_stat_id_seq OWNER TO postgres;

--
-- TOC entry 3042 (class 0 OID 0)
-- Dependencies: 207
-- Name: playerstatistics_stat_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.playerstatistics_stat_id_seq OWNED BY public.playerstatistics.stat_id;


--
-- TOC entry 204 (class 1259 OID 16404)
-- Name: teams; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.teams (
    team_id integer NOT NULL,
    team_name character varying(255) NOT NULL
);


ALTER TABLE public.teams OWNER TO postgres;

--
-- TOC entry 203 (class 1259 OID 16402)
-- Name: teams_team_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.teams_team_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.teams_team_id_seq OWNER TO postgres;

--
-- TOC entry 3043 (class 0 OID 0)
-- Dependencies: 203
-- Name: teams_team_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.teams_team_id_seq OWNED BY public.teams.team_id;


--
-- TOC entry 210 (class 1259 OID 16438)
-- Name: teamstatistics; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.teamstatistics (
    stat_id integer NOT NULL,
    match_id integer,
    team_id integer,
    passes integer,
    through_balls integer,
    shots integer
);


ALTER TABLE public.teamstatistics OWNER TO postgres;

--
-- TOC entry 209 (class 1259 OID 16436)
-- Name: teamstatistics_stat_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.teamstatistics_stat_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.teamstatistics_stat_id_seq OWNER TO postgres;

--
-- TOC entry 3044 (class 0 OID 0)
-- Dependencies: 209
-- Name: teamstatistics_stat_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.teamstatistics_stat_id_seq OWNED BY public.teamstatistics.stat_id;


--
-- TOC entry 2877 (class 2604 OID 16415)
-- Name: matches match_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.matches ALTER COLUMN match_id SET DEFAULT nextval('public.matches_match_id_seq'::regclass);


--
-- TOC entry 2875 (class 2604 OID 16399)
-- Name: players player_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.players ALTER COLUMN player_id SET DEFAULT nextval('public.players_player_id_seq'::regclass);


--
-- TOC entry 2878 (class 2604 OID 16423)
-- Name: playerstatistics stat_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.playerstatistics ALTER COLUMN stat_id SET DEFAULT nextval('public.playerstatistics_stat_id_seq'::regclass);


--
-- TOC entry 2876 (class 2604 OID 16407)
-- Name: teams team_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.teams ALTER COLUMN team_id SET DEFAULT nextval('public.teams_team_id_seq'::regclass);


--
-- TOC entry 2879 (class 2604 OID 16441)
-- Name: teamstatistics stat_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.teamstatistics ALTER COLUMN stat_id SET DEFAULT nextval('public.teamstatistics_stat_id_seq'::regclass);


--
-- TOC entry 3029 (class 0 OID 16412)
-- Dependencies: 206
-- Data for Name: matches; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.matches (match_id, season) FROM stdin;
1	2020/2021
2	2019/2020
3	2018/2019
4	2003/2004
5	2020/2021
6	2019/2020
7	2018/2019
8	2003/2004
\.


--
-- TOC entry 3025 (class 0 OID 16396)
-- Dependencies: 202
-- Data for Name: players; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.players (player_id, player_name) FROM stdin;
1	Lionel Messi
2	Karim Benzema
3	Luis Suarez
4	Youssef En-Nesyri
5	Bruno Fernandes
6	Frank Lampard
\.


--
-- TOC entry 3031 (class 0 OID 16420)
-- Dependencies: 208
-- Data for Name: playerstatistics; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.playerstatistics (stat_id, match_id, player_id, shots, first_time_shots, through_balls, successful_dribbles, dribbled_past, intended_recipient_of_passes, average_xg) FROM stdin;
1	1	1	150	45	20	80	5	300	0.92
2	1	2	140	40	18	70	10	250	0.88
3	1	3	130	35	15	60	15	200	0.85
4	1	4	120	30	12	55	20	150	0.82
5	2	1	160	50	22	85	4	310	0.95
6	2	2	145	42	17	75	9	260	0.90
7	2	3	135	38	14	65	14	210	0.87
8	2	4	125	33	11	60	19	160	0.83
9	3	1	155	48	21	82	6	305	0.93
10	3	2	138	41	16	72	11	255	0.89
11	3	3	128	37	13	67	16	205	0.86
12	3	4	118	32	10	58	21	155	0.81
13	4	5	110	29	9	50	25	140	0.78
14	4	6	105	27	7	45	30	130	0.75
\.


--
-- TOC entry 3027 (class 0 OID 16404)
-- Dependencies: 204
-- Data for Name: teams; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.teams (team_id, team_name) FROM stdin;
1	FC Barcelona
2	Real Madrid
3	Atletico Madrid
4	Sevilla FC
5	Manchester United
6	Chelsea FC
\.


--
-- TOC entry 3033 (class 0 OID 16438)
-- Dependencies: 210
-- Data for Name: teamstatistics; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.teamstatistics (stat_id, match_id, team_id, passes, through_balls, shots) FROM stdin;
1	1	1	10200	80	320
2	1	2	9800	75	310
3	1	3	9600	70	300
4	1	4	9400	65	290
5	2	1	10300	82	325
6	2	2	9900	77	315
7	2	3	9700	72	305
8	2	4	9500	67	295
9	3	1	10400	85	330
10	3	2	10000	80	320
11	3	3	9800	75	310
12	3	4	9600	70	300
13	4	5	9500	68	280
14	4	6	9300	63	270
\.


--
-- TOC entry 3045 (class 0 OID 0)
-- Dependencies: 205
-- Name: matches_match_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.matches_match_id_seq', 8, true);


--
-- TOC entry 3046 (class 0 OID 0)
-- Dependencies: 201
-- Name: players_player_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.players_player_id_seq', 6, true);


--
-- TOC entry 3047 (class 0 OID 0)
-- Dependencies: 207
-- Name: playerstatistics_stat_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.playerstatistics_stat_id_seq', 14, true);


--
-- TOC entry 3048 (class 0 OID 0)
-- Dependencies: 203
-- Name: teams_team_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.teams_team_id_seq', 6, true);


--
-- TOC entry 3049 (class 0 OID 0)
-- Dependencies: 209
-- Name: teamstatistics_stat_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.teamstatistics_stat_id_seq', 14, true);


--
-- TOC entry 2885 (class 2606 OID 16417)
-- Name: matches matches_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.matches
    ADD CONSTRAINT matches_pkey PRIMARY KEY (match_id);


--
-- TOC entry 2881 (class 2606 OID 16401)
-- Name: players players_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.players
    ADD CONSTRAINT players_pkey PRIMARY KEY (player_id);


--
-- TOC entry 2887 (class 2606 OID 16425)
-- Name: playerstatistics playerstatistics_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.playerstatistics
    ADD CONSTRAINT playerstatistics_pkey PRIMARY KEY (stat_id);


--
-- TOC entry 2883 (class 2606 OID 16409)
-- Name: teams teams_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.teams
    ADD CONSTRAINT teams_pkey PRIMARY KEY (team_id);


--
-- TOC entry 2889 (class 2606 OID 16443)
-- Name: teamstatistics teamstatistics_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.teamstatistics
    ADD CONSTRAINT teamstatistics_pkey PRIMARY KEY (stat_id);


--
-- TOC entry 2890 (class 2606 OID 16426)
-- Name: playerstatistics playerstatistics_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.playerstatistics
    ADD CONSTRAINT playerstatistics_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.matches(match_id);


--
-- TOC entry 2891 (class 2606 OID 16431)
-- Name: playerstatistics playerstatistics_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.playerstatistics
    ADD CONSTRAINT playerstatistics_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.players(player_id);


--
-- TOC entry 2892 (class 2606 OID 16444)
-- Name: teamstatistics teamstatistics_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.teamstatistics
    ADD CONSTRAINT teamstatistics_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.matches(match_id);


--
-- TOC entry 2893 (class 2606 OID 16449)
-- Name: teamstatistics teamstatistics_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.teamstatistics
    ADD CONSTRAINT teamstatistics_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.teams(team_id);


-- Completed on 2024-03-15 13:43:47

--
-- PostgreSQL database dump complete
--

