<template lang="html">
  <div id="methods">
    <div class="modal" id="config-modal">
      <div class="modal-content">
        <div v-if="active_cfg">
          <h4>
            {{active_cfg.config.namespace}}/{{active_cfg.config.name}}
            <span class="right">
              <i v-if="edit_on" class="material-icons green-text">check</i>
              <i class="material-icons" v-bind:class="edit_on?'orange-text text-darken-3':''"v-on:click.prevent="edit_on = !edit_on">
                {{edit_on?"cancel":"edit"}}
              </i>
              <i class="material-icons red-text">delete</i>
            </span>
          </h4>
          <div style="border-radius: 10px; padding-top: 15px; border: 1px solid grey" class="grey lighten-4">
            <div class="row">
              <div class="col s2">
                Method:
              </div>
              <div class="col s10">
                <a v-bind:href="'https://portal.firecloud.org/#methods/'+active_cfg.config.methodRepoMethod.methodNamespace+'/'+active_cfg.config.methodRepoMethod.methodName+'/'+active_cfg.config.methodRepoMethod.methodVersion" target="_blank" rel="noopener">
                  {{active_cfg.config.methodRepoMethod.methodNamespace}}/{{active_cfg.config.methodRepoMethod.methodName}}
                </a>
              </div>
            </div>
            <div class="row">
              <div class="col s2">
                Entity Type:
              </div>
              <div class="col s9">
                {{active_cfg.config.rootEntityType}}
              </div>
            </div>
            <div class="row">
              <div class="col s12">
                <a
                v-bind:href="'https://portal.firecloud.org/#workspaces/'+namespace+'/'+workspace+'/method-configs/'+active_cfg.config.namespace+'/'+active_cfg.config.name"
                target="_blank" rel="noopener"
                >
                View config in Firecloud
                </a>
              </div>
            </div>
          </div>
          <div style="margin-top: 15px; margin-bottom: 0px;" class="row" v-on:click.prevent="toggle_wdl">
            <div v-if="!show_wdl" class="col s4" >
              <span>
                <i class="material-icons">keyboard_arrow_right</i>
                WDL
              </span>
            </div>
            <div v-else class="col s4">
              <span>
                <i class="material-icons">keyboard_arrow_down</i>
                WDL
              </span>
            </div>
            <div class="col s8 right">

            </div>
          </div>
          <div class="row" v-if="active_cfg.wdl && show_wdl">
            <div style="border-radius: 10px; padding: 15px; border: 1px solid grey" class="log-contianer">
              <pre style="overflow-x: scroll;">{{active_cfg.wdl}}</pre>
            </div>
          </div>
          <div class="row" style="margin-top: 0px;">
            <div class="col s6">
              <h5>Inputs</h5>
            </div>
            <div class="col s6">
              <h5>Outputs</h5>
            </div>
          </div>
          <div class="row">
            <form class="col s6">
              <div class="row" v-for="key in lodash.keys(active_cfg.config.inputs)">
                <div class="input-field col s11" style="margin: 0px;">
                  <input type="text" v-bind:id="key" v-model="active_cfg.config.inputs[key]" v-bind:disabled="!edit_on"/>
                  <label style="z-index: -1;">{{key}}</label>
                </div>
              </div>
            </form>
            <form class="col s6">
              <div class="row" v-for="key in lodash.keys(active_cfg.config.outputs)">
                <div class="input-field col s11" style="margin: 0px;">
                  <input type="text" v-bind:id="key" v-model="active_cfg.config.outputs[key]" v-bind:disabled="!edit_on"/>
                  <label style="z-index: -1;">{{key}}</label>
                </div>
              </div>
            </form>
          </div>
        </div>
      </div>
      <!-- <div class="modal-footer">
        <a href="#">YES</a>
        <a href="#">NO</a>
      </div> -->
    </div>
    <h4>
      <router-link :to="{name: 'workspace', params: {namespace: namespace, workspace: workspace}}">
        {{namespace}}/{{workspace}}
      </router-link>
    </h4>
    <h3>Methods</h3>
    <strong>(Placeholder button: Add new configuration)</strong>
    <table>
      <thead>
        <tr>
          <th>Configuration</th>
          <th>Method</th>
          <th>Entity Type</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="config in configs">
          <td>
            <a href="#" v-on:click.prevent="display_config(config.namespace+'/'+config.name, false)">
              {{config.namespace}}/{{config.name}}
            </a>

          </td>
          <td>{{config.methodRepoMethod.methodNamespace}}/{{config.methodRepoMethod.methodName}} (Snapshot {{config.methodRepoMethod.methodVersion}})</td>
          <td>{{config.rootEntityType}}</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script>
import axios from'axios'
import _ from 'lodash'
export default {
  props: ['namespace', 'workspace'],
  data() {
    return {
      lodash:_,
      configs: null,
      active_cfg: null,
      edit_dirty: null,
      edit_on: false,
      show_wdl: false
    }
  },
  created() {
    this.init(this.namespace, this.workspace);
    window.$('.modal').modal();
  },
  methods: {
    init(namespace, workspace) {
      this.configs = null;
      this.active_cfg = null;
      this.edit_dirty = null;
      this.edit_on = false;
      this.show_wdl = false;
      this.get_configs(namespace, workspace);
    },
    get_configs(ns, ws) {
      axios.get(API_URL+'/api/v1/workspaces/'+ns+'/'+ws+'/configs')
        .then(response => {
          console.log(response.data);
          this.configs = response.data
        })
        .catch(error => {
          console.error(error)
          window.materialize.toast({
            html: "Failed to get method configs"
          })
        })
    },
    display_config(config_slug, editable) {
      console.log(config_slug);
      this.active_cfg = null;
      this.edit_dirty = null;
      this.edit_on = editable;
      this.show_wdl = false;
      window.materialize.toast({
        html: "Loading config "+config_slug,
        displayLength: 1000
      });
      axios.get(API_URL + '/api/v1/workspaces/'+this.namespace+'/'+this.workspace+'/configs/'+config_slug)
        .then(response => {
          console.log(response.data);
          this.active_cfg = response.data;

          setTimeout(() => {
            window.$("#config-modal").modal();
            window.materialize.updateTextFields();
            window.$("#config-modal").modal('open');
          }, 250)

        })
        .catch(error => {
          console.error(error);
          window.materialize.toast({
            html: "Failed to get config "+config_slug
          });
        })
    },
    toggle_wdl() {
      this.show_wdl = !this.show_wdl;
    }
  },
  beforeRouteUpdate(to, from, next) {
    console.log("Update!");
    this.init(to.params.namespace, to.params.workspace);
    next();
  }
}
</script>

<style lang="css">
  div.log-container {
    max-height: 250px;
    border-radius: 8px;
    overflow-y: auto;
    /* margin: 1em;
    padding: 1em; */
    /* border: 1px solid black; */
    padding-left: 20px;
    font-family: monospace;
    white-space: pre-wrap;
    font-size: 90%;
  }
</style>
