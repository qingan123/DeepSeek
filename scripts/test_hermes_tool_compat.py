#!/usr/bin/env python3
"""Offline compatibility regression for Hermes tool schemas and 60089 parser."""
from __future__ import annotations
import json, os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.api.openai import (
    ConversationBinding,
    _canonicalize_tool_call,
    _parse_tool_calls,
    _fallback_tool_text,
    _tool_function_names,
    _tool_prompt,
    _tool_schema_delivery_mode,
    _tool_schema_reuse_prompt,
    _tool_schema,
)

DEFAULT_DEFS_PATH = Path(__file__).resolve().parent / 'fixtures' / 'hermes-tool-definitions.minimal.json'
DEFS_PATH = Path(os.environ.get('HERMES_TOOL_DEFS', str(DEFAULT_DEFS_PATH)))

def fn_of(d):
    return d.get('function', d)

def sample(schema):
    if not isinstance(schema, dict): return 'x'
    if schema.get('enum'): return schema['enum'][0]
    t=schema.get('type')
    if isinstance(t,list): t=next((x for x in t if x!='null'),'string')
    if t=='integer': return 1
    if t=='number': return 1.5
    if t=='boolean': return True
    if t=='array': return []
    if t=='object': return {}
    return 'compat_value'

def dsml_value(v):
    if isinstance(v,(dict,list,bool,int,float)) or v is None:
        return json.dumps(v,ensure_ascii=False)
    return str(v)

def parsed_args(call):
    return json.loads(call['function']['arguments'])

def main():
    raw_defs=json.loads(DEFS_PATH.read_text(encoding='utf-8'))
    defs=raw_defs.get('tools', raw_defs) if isinstance(raw_defs, dict) else raw_defs
    if not isinstance(defs, list) or not defs:
        raise SystemExit(f'No tool definitions found in {DEFS_PATH}')
    names={fn_of(x)['name'] for x in defs}
    # Hermes converts Chat schemas to Responses top-level function schemas
    # before POST /v1/responses. Both shapes must remain equivalent.
    response_defs=[]
    for d in defs:
        f=fn_of(d)
        response_defs.append({
          'type':'function','name':f['name'],
          'description':f.get('description',''),'strict':False,
          'parameters':f.get('parameters') or {'type':'object','properties':{}},
        })
    failures=[]; exact_dsml=0; exact_json=0
    response_dsml=0
    if _tool_function_names(response_defs) != names:
        failures.append(f'RESPONSES names: {_tool_function_names(response_defs)} != {names}')
    response_prompt=_tool_prompt(response_defs, profile='hermes')
    for name in names:
        if f'- {name}:' not in response_prompt:
            failures.append(f'RESPONSES prompt missing {name}')
        if not _tool_schema(response_defs,name):
            failures.append(f'RESPONSES schema missing {name}')
    for d in defs:
        f=fn_of(d); name=f['name']; schema=f.get('parameters') or {}
        props=schema.get('properties') or {}; required=schema.get('required') or []
        args={k:sample(props.get(k,{})) for k in required}
        params=''.join(f'<|DSML| parameter name="{k}">{dsml_value(v)}</|DSML| parameter>' for k,v in args.items())
        text=f'<|DSML| tool_calls><|DSML| invoke name="{name}">{params}</|DSML| invoke></|DSML| tool_calls>'
        calls=_parse_tool_calls(text,defs,{'arg':False,'path':False,'mojibake':False})
        if len(calls)!=1 or calls[0]['function']['name']!=name:
            failures.append(f'DSML {name}: {calls}')
        else:
            got=parsed_args(calls[0])
            if any(k not in got for k in required): failures.append(f'DSML required {name}: {got}')
            exact_dsml+=1
        payload=json.dumps({'tool_calls':[{'type':'function','function':{'name':name,'arguments':json.dumps(args)}}]})
        calls=_parse_tool_calls(payload,defs,{'arg':False,'path':False,'mojibake':False})
        if len(calls)!=1 or calls[0]['function']['name']!=name:
            failures.append(f'JSON {name}: {calls}')
        else: exact_json+=1

        calls=_parse_tool_calls(text,response_defs,{'arg':False,'path':False,'mojibake':False})
        if len(calls)!=1 or calls[0]['function']['name']!=name:
            failures.append(f'RESPONSES DSML {name}: {calls}')
        else:
            got=parsed_args(calls[0])
            if any(k not in got for k in required): failures.append(f'RESPONSES required {name}: {got}')
            response_dsml+=1

    alias_cases=[
      ('execute_command',{'command':'printf ok'},'terminal',{'command':'printf ok'}),
      ('shell_exec',{'command':'printf ok'},'terminal',{'command':'printf ok'}),
      ('bash',{'command':'printf ok'},'terminal',{'command':'printf ok'}),
      ('execute_code',{'command':'printf ok'},'terminal',{'command':'printf ok'}),
      ('terminal',{'code':'print(1)'},'execute_code',{'code':'print(1)'}),
      ('read',{'filePath':'/tmp/x'},'read_file',{'path':'/tmp/x'}),
      ('write',{'filePath':'/tmp/x','content':'y'},'write_file',{'path':'/tmp/x','content':'y'}),
      ('edit',{'filePath':'/tmp/x','oldString':'a','newString':'b','replaceAll':'true'},'patch',{'path':'/tmp/x','old_string':'a','new_string':'b','replace_all':True,'mode':'replace'}),
    ]
    alias_ok=0
    for source,args,target,expected in alias_cases:
        name,canon=_canonicalize_tool_call(source,args,defs)
        text=json.dumps({'tool_calls':[{'function':{'name':source,'arguments':json.dumps(args)}}]})
        calls=_parse_tool_calls(text,defs,{'arg':False,'path':False,'mojibake':False})
        got=parsed_args(calls[0]) if calls else None
        if name!=target or not calls or calls[0]['function']['name']!=target or got!=expected:
            failures.append(f'ALIAS {source}: canonical=({name},{canon}) parsed={calls} expected={target},{expected}')
        else: alias_ok+=1

    deferred='<|DSML| tool_calls><|DSML| invoke name="mcp__bt_ops__list_resources"><|DSML| parameter name="page">1</|DSML| parameter><|DSML| parameter name="limit">20</|DSML| parameter></|DSML| invoke></|DSML| tool_calls>'
    wrapped=_parse_tool_calls(deferred,response_defs,{'arg':False,'path':False,'mojibake':False})
    if len(wrapped)!=1 or wrapped[0]['function']['name']!='tool_call':
        failures.append(f'DEFERRED MCP wrap: {wrapped}')
    else:
        wrapped_args=parsed_args(wrapped[0])
        if wrapped_args != {'name':'mcp__bt_ops__list_resources','arguments':{'page':1,'limit':20}}:
            failures.append(f'DEFERRED MCP args: {wrapped_args}')

    fallback=_fallback_tool_text(deferred,'clean_text')
    if fallback.strip()=='120' or '<|DSML|' in fallback:
        failures.append(f'FALLBACK leaked parameter fragments: {fallback!r}')

    # Every new user turn must register the complete schema. A direct tool
    # result continuation in the same bound DeepSeek session may reuse it.
    binding=ConversationBinding(
        conversation_id=9001,turn_id=9002,local_conversation_id='compat-schema-session',
        baidu_session_id='deepseek-session-1',rank=1,
    )
    new_messages=[{'role':'user','content':'run the compatibility probe'}]
    mode,reason,schema_hash=_tool_schema_delivery_mode(
        new_messages,response_defs,'compat-schema-cache',binding
    )
    if mode!='full' or reason!='new_user_turn' or not schema_hash:
        failures.append(f'SCHEMA new user: {(mode,reason,schema_hash)}')

    continuation=[
        {'role':'user','content':'run the compatibility probe'},
        {'role':'assistant','content':'','tool_calls':[{
            'id':'call_schema_probe','type':'function',
            'function':{'name':'terminal','arguments':'{"command":"printf ok"}'},
        }]},
        {'role':'tool','tool_call_id':'call_schema_probe','name':'terminal',
         'content':'{"output":"ok","exit_code":0,"error":null}'},
    ]
    mode,reason,reused_hash=_tool_schema_delivery_mode(
        continuation,response_defs,'compat-schema-cache',binding
    )
    if mode!='reuse' or reason!='same_task_tool_result' or reused_hash!=schema_hash:
        failures.append(f'SCHEMA continuation reuse: {(mode,reason,reused_hash)}')
    reuse_prompt=_tool_schema_reuse_prompt(response_defs,profile='hermes')
    if '; parameters=' in reuse_prompt or 'Registered tool names:' not in reuse_prompt or 'terminal' not in reuse_prompt:
        failures.append(f'SCHEMA reuse prompt malformed: {reuse_prompt[:500]}')

    validation_error=[*continuation[:-1],{
        'role':'tool','tool_call_id':'call_schema_probe','name':'terminal',
        'content':"tool_call to 'terminal' is missing required argument(s): command. The tool was NOT invoked. Parameters schema: {...}",
    }]
    mode,reason,_=_tool_schema_delivery_mode(
        validation_error,response_defs,'compat-schema-cache-validation',binding
    )
    if mode!='full' or reason not in {'schema_cache_miss_or_restart','tool_schema_validation_error'}:
        failures.append(f'SCHEMA validation fallback: {(mode,reason)}')

    # Seed then prove that a validation error forces a full resend rather than
    # taking the ordinary same-session continuation path.
    _tool_schema_delivery_mode(new_messages,response_defs,'compat-schema-cache-validation-2',binding)
    mode,reason,_=_tool_schema_delivery_mode(
        validation_error,response_defs,'compat-schema-cache-validation-2',binding
    )
    if mode!='full' or reason!='tool_schema_validation_error':
        failures.append(f'SCHEMA validation seeded fallback: {(mode,reason)}')

    no_session=ConversationBinding(
        conversation_id=9001,turn_id=9003,local_conversation_id='compat-schema-session',
        baidu_session_id='',rank=1,
    )
    _tool_schema_delivery_mode(new_messages,response_defs,'compat-schema-cache-no-session',no_session)
    mode,reason,_=_tool_schema_delivery_mode(
        continuation,response_defs,'compat-schema-cache-no-session',no_session
    )
    if mode!='full' or reason!='new_or_reset_upstream_session':
        failures.append(f'SCHEMA reset session fallback: {(mode,reason)}')

    print(json.dumps({
      'definitions_path':str(DEFS_PATH),'fixture_mode':DEFS_PATH==DEFAULT_DEFS_PATH,
      'tool_count':len(defs),'exact_dsml_pass':exact_dsml,
      'exact_json_pass':exact_json,'responses_dsml_pass':response_dsml,
      'alias_pass':alias_ok,'deferred_mcp_wrap_pass':1 if not any(x.startswith('DEFERRED MCP') for x in failures) else 0,
      'schema_reuse_pass':1 if not any(x.startswith('SCHEMA ') for x in failures) else 0,
      'failures':failures,
    },ensure_ascii=False,indent=2))
    raise SystemExit(1 if failures else 0)

if __name__=='__main__': main()
