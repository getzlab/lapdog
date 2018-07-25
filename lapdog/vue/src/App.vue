<template>
  <div id="app">
    <header>
      <div class="sidenav" id="slide-out" style="width: 50% !important;">
        <div class="user-view center-align">
          <h3>Workspaces</h3>
        </div>
        <div class="container">
          <div class="collection">
            <a class="collection-item"><i class="material-icons">add_box</i> Create new Workspace</a>
          </div>
          <div class="divider">
          </div>
          <div class="collection">
            <div class="collection-item" v-if="!workspaces">
              <div class="progress">
                <div class="indeterminate"></div>
              </div>
            </div>
            <router-link class="collection-item sidenav-close" v-for="workspace in workspaces" active-class="active"
              :to="{name: 'workspace', params: {namespace: workspace.namespace, workspace: workspace.name}}">
              {{workspace.namespace}}/{{workspace.name}}
            </router-link>
          </div>
        </div>
      </div>
      <nav class="blue darken-2">
        <div class="nav-wrapper">
          <router-link href="#" class="brand-logo right link" :to="'/'">Lapdog</router-link>
          <ul class="left">
            <li>
              <a data-target="slide-out" class="sidenav-trigger show-on-large"><i class="material-icons">menu</i></a>
            </li>
          </ul>
        </div>
      </nav>
      <!-- <ul id="slide-out" class="sidenav">
        <li><div class="user-view">
          <h3>Workspaces</h3>
        </div></li>
        <li><i class="material-icons">add_box</i> Create new Workspace</li>
      </ul> -->

      <!-- <a data-target="slide-out" class="sidenav-trigger left">Menu</a> -->
    </header>
    <main>
      <aside class="sidebar">
      </aside>
      <div class="content container">
        <router-view></router-view>
      </div>
    </main>
    <footer class="grey lighten-1">
      <div class="row">
        <div class="col s11 offset-s1">
          Powered by Dalmatian
        </div>
      </div>
    </footer>
  </div>
</template>

<!-- <script type="text/javascript">
  console.log(window.$)
</script> -->

<script>
import axios from'axios'
export default {
  data() {
    return {
      workspaces: null
    }
  },

  created() {
    this.getStatus();
  },

  methods: {
    getStatus() {
      axios.get('http://localhost:4201/api/v1/workspaces')
        .then(response => {
          this.workspaces = response.data;
          console.log(this.workspaces[0])
        })
        .catch(error => {
          console.error("FAIL!")
        })
    }
  }
}
</script>
